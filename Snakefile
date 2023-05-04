serotypes = ['all', 'denv1', 'denv2', 'denv3', 'denv4']

rule all:
    input:
        auspice_json = expand("auspice/dengue_{serotype}.json", serotype=serotypes)

rule files:
    params:
        dropped_strains = "config/dropped_strains.txt",
        reference = "config/reference_dengue_{serotype}.gb",
        auspice_config = "config/auspice_config_{serotype}.json"

files = rules.files.params

def download_serotype(w):
    serotype = {
        'all': 'all',
        'denv1': 'Dengue_virus_1',
        'denv2': 'Dengue_virus_2',
        'denv3': 'Dengue_virus_3',
        'denv4': 'Dengue_virus_4'
    }
    return serotype[w.serotype]

def filter_sequences_per_group(w):
    sequences_per_group = {
        'all': '10',
        'denv1': '36',
        'denv2': '36',
        'denv3': '36',
        'denv4': '36'
    }
    return sequences_per_group[w.serotype]

def traits_columns(w):
    traits = {
        'all': 'region',
        'denv1': 'country region',
        'denv2': 'country region',
        'denv3': 'country region',
        'denv4': 'country region'
    }
    return traits[w.serotype]

def clade_defs(w):
    defs = {
        'all': 'config/clades_serotypes.tsv',
        'denv1': 'config/clades_genotypes.tsv',
        'denv2': 'config/clades_genotypes.tsv',
        'denv3': 'config/clades_genotypes.tsv',
        'denv4': 'config/clades_genotypes.tsv'
    }
    return defs[w.serotype]

rule download:
    message: "Downloading sequences and metadata from data.nextstrain.org"
    output:
        sequences = "data/sequences_{serotype}.fasta.zst",
        metadata = "data/metadata_{serotype}.tsv.zst"

    params:
        sequences_url = "https://data.nextstrain.org/files/dengue/sequences_{serotype}.fasta.zst",
        metadata_url = "https://data.nextstrain.org/files/dengue/metadata_{serotype}.tsv.zst"
    shell:
        """
        curl -fsSL --compressed {params.sequences_url:q} --output {output.sequences}
        curl -fsSL --compressed {params.metadata_url:q} --output {output.metadata}
        """

rule decompress:
    message: "Parsing fasta into sequences and metadata"
    input:
        sequences = "data/sequences_{serotype}.fasta.zst",
        metadata = "data/metadata_{serotype}.tsv.zst"
    output:
        sequences = "results/sequences_{serotype}.fasta",
        metadata = "results/metadata_{serotype}.tsv"
    shell:
        """
        zstd -d -c {input.sequences} > {output.sequences}
        zstd -d -c {input.metadata} > {output.metadata}
        """

rule filter:
    message:
        """
        Filtering to
          - {params.sequences_per_group} sequence(s) per {params.group_by!s}
          - excluding strains in {input.exclude}
          - minimum genome length of {params.min_length}
          - excluding strains with missing region, country or date metadata
        """
    input:
        sequences = "results/sequences_{serotype}.fasta",
        metadata = "results/metadata_{serotype}.tsv",
        exclude = files.dropped_strains
    output:
        sequences = "results/filtered_{serotype}.fasta"
    params:
        group_by = "year region",
        sequences_per_group = filter_sequences_per_group,
        min_length = 5000
    shell:
        """
        augur filter \
            --sequences {input.sequences} \
            --metadata {input.metadata} \
            --exclude {input.exclude} \
            --output {output.sequences} \
            --group-by {params.group_by} \
            --sequences-per-group {params.sequences_per_group} \
            --min-length {params.min_length} \
            --exclude-where country=? region=? date=? \
        """

rule align:
    message:
        """
        Aligning sequences to {input.reference}
          - filling gaps with N
        """
    input:
        sequences = "results/filtered_{serotype}.fasta",
        reference = files.reference
    output:
        alignment = "results/aligned_{serotype}.fasta"
    shell:
        """
        augur align \
            --sequences {input.sequences} \
            --reference-sequence {input.reference} \
            --output {output.alignment} \
            --fill-gaps \
            --remove-reference \
            --nthreads 1
        """

rule tree:
    message: "Building tree"
    input:
        alignment = "results/aligned_{serotype}.fasta"
    output:
        tree = "results/tree-raw_{serotype}.nwk"
    shell:
        """
        augur tree \
            --alignment {input.alignment} \
            --output {output.tree} \
            --nthreads 1
        """

rule refine:
    message:
        """
        Refining tree
          - estimate timetree
          - use {params.coalescent} coalescent timescale
          - estimate {params.date_inference} node dates
          - filter tips more than {params.clock_filter_iqd} IQDs from clock expectation
        """
    input:
        tree = "results/tree-raw_{serotype}.nwk",
        alignment = "results/aligned_{serotype}.fasta",
        metadata = "results/metadata_{serotype}.tsv"
    output:
        tree = "results/tree_{serotype}.nwk",
        node_data = "results/branch-lengths_{serotype}.json"
    params:
        coalescent = "const",
        date_inference = "marginal",
        clock_filter_iqd = 4
    shell:
        """
        augur refine \
            --tree {input.tree} \
            --alignment {input.alignment} \
            --metadata {input.metadata} \
            --output-tree {output.tree} \
            --output-node-data {output.node_data} \
            --timetree \
            --coalescent {params.coalescent} \
            --date-confidence \
            --date-inference {params.date_inference} \
            --clock-filter-iqd {params.clock_filter_iqd}
        """

rule ancestral:
    message: "Reconstructing ancestral sequences and mutations"
    input:
        tree = "results/tree_{serotype}.nwk",
        alignment = "results/aligned_{serotype}.fasta"
    output:
        node_data = "results/nt-muts_{serotype}.json"
    params:
        inference = "joint"
    shell:
        """
        augur ancestral \
            --tree {input.tree} \
            --alignment {input.alignment} \
            --output-node-data {output.node_data} \
            --inference {params.inference}
        """

rule translate:
    message: "Translating amino acid sequences"
    input:
        tree = "results/tree_{serotype}.nwk",
        node_data = "results/nt-muts_{serotype}.json",
        reference = files.reference
    output:
        node_data = "results/aa-muts_{serotype}.json"
    shell:
        """
        augur translate \
            --tree {input.tree} \
            --ancestral-sequences {input.node_data} \
            --reference-sequence {input.reference} \
            --output {output.node_data} \
        """

rule traits:
    message:
        """
        Inferring ancestral traits for {params.columns!s}
          - increase uncertainty of reconstruction by {params.sampling_bias_correction} to partially account for sampling bias
        """
    input:
        tree = "results/tree_{serotype}.nwk",
        metadata = "results/metadata_{serotype}.tsv"
    output:
        node_data = "results/traits_{serotype}.json",
    params:
        columns = traits_columns,
        sampling_bias_correction = 3
    shell:
        """
        augur traits \
            --tree {input.tree} \
            --metadata {input.metadata} \
            --output {output.node_data} \
            --columns {params.columns} \
            --confidence \
            --sampling-bias-correction {params.sampling_bias_correction}
        """

rule clades:
    message: "Annotating serotypes / genotypes"
    input:
        tree = "results/tree_{serotype}.nwk",
        nt_muts = "results/nt-muts_{serotype}.json",
        aa_muts = "results/aa-muts_{serotype}.json",
        clade_defs = clade_defs,
    output:
        clades = "results/clades_{serotype}.json"
    shell:
        """
        augur clades \
            --tree {input.tree} \
            --mutations {input.nt_muts} {input.aa_muts} \
            --clades {input.clade_defs} \
            --output {output.clades}
        """

rule export:
    message: "Exporting data files for for auspice"
    input:
        tree = "results/tree_{serotype}.nwk",
        metadata = "results/metadata_{serotype}.tsv",
        branch_lengths = "results/branch-lengths_{serotype}.json",
        traits = "results/traits_{serotype}.json",
        clades = "results/clades_{serotype}.json",
        nt_muts = "results/nt-muts_{serotype}.json",
        aa_muts = "results/aa-muts_{serotype}.json",
        auspice_config = files.auspice_config
    output:
        auspice_json = "auspice/dengue_{serotype}.json"
    shell:
        """
        augur export v2 \
            --tree {input.tree} \
            --metadata {input.metadata} \
            --node-data {input.branch_lengths} {input.traits} {input.clades} {input.nt_muts} {input.aa_muts} \
            --auspice-config {input.auspice_config} \
            --include-root-sequence \
            --output {output.auspice_json}
        """

rule clean:
    message: "Removing directories: {params}"
    params:
        "results ",
        "auspice"
    shell:
        "rm -rfv {params}"
