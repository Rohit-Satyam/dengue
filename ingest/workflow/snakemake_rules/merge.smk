rule pull_cache:
  output:
    cache_metadata = "data/cache_metadata.tsv",
    cache_sequences = "data/cache_sequences.fasta",
  shell:
    """
    # Pull current metadata from Terra
    gsutil cp 'gs://fc-29a44672-3ae3-4dc8-8c5c-e0c71c36fda3/A_Pathogen_Dengue/metadata_all.tsv' {output.cache_metadata}
    gsutil cp 'gs://fc-29a44672-3ae3-4dc8-8c5c-e0c71c36fda3/A_Pathogen_Dengue/sequences_all.fasta' {output.cache_sequences}
    """

rule merge_sequences:
    input:
        cache_sequences = "data/cache_sequences.fasta",
        sequences = "data/sequences_all.fasta",
    output:
        sequences = "terra/sequences_all.fasta",
    shell:
        """
        # Merge new sequences with current sequences
        cat {input.sequences} {input.cache_sequences} > temp.fasta

        # This is a temporary solution to the problem of duplicate sequences
        # smof is an external dependency not in Docker, nor is this expected to be a permanent solution
        smof uniq --first-header temp.fasta > {output.sequences}
        rm temp.fasta
        """

rule merge_metadata:
    input:
        cache_metadata = "data/cache_metadata.tsv",
        metadata = "data/metadata_all.tsv",
    output:
        metadata = "data/merged_metadata.tsv",
    shell:
        """
        # Merge new metadata with current metadata
        python bin/uniq_merge.py \
          --cache {input.cache_metadata} \
          --new {input.metadata} \
          --groupby_col accession \
          --outfile data/temp.tsv
          
        cat data/temp.tsv \
        | csvtk sort -t -k updated:r \
        > {output.metadata}
        """

rule fix_titles:
    """Entrez call, will take a while"""
    input:
        metadata = "data/metadata_all.tsv",
    output:
        metadata = "data/fixed_metadata_all.tsv"
    shell:
        """
        cat {input.metadata} \
        | bin/tsv-to-ndjson \
        | python bin/transform-citations.py \
          --group-size 200 \
          --genbank-id-field accession \
        | python bin/ndjson-to-tsv \
          --metadata-columns accession journal title \
          --metadata {output.metadata} \
          --id-field accession
        """

rule fix_citations:
    input:
        cache_metadata = "data/cache_metadata.tsv",
        new_metadata = "data/fixed_metadata_all.tsv",
        metadata = "data/merged_metadata.tsv",
    output:
        metadata = "terra/metadata_all.tsv"
    shell:
        """
        # journal
        dos2unix {input.metadata}
        dos2unix {input.cache_metadata}
        dos2unix {input.new_metadata}

        tsv-select -H \
          -f accession,journal,title {input.cache_metadata} \
          > data/cache_citations.tsv
        cat {input.new_metadata} \
        | tsv-select -H -f accession,journal,title \
        > data/new_citations.tsv

        tsv-append -H data/cache_citations.tsv data/new_citations.tsv > data/citations.tsv

        cat {input.metadata} \
        | tsv-join -H \
        --filter-file data/citations.tsv \
        --key-fields accession \
        --append-fields journal,title \
        --allow-duplicate-keys \
        --write-all -1 \
        > {output.metadata}
        """
          
rule split_files:
    input:
        metadata = "terra/metadata_all.tsv",
        sequences = "terra/sequences_all.fasta",
    output:      
        metadata_denv1 = "terra/metadata_denv1.tsv",
        sequences_denv1 = "terra/sequences_denv1.fasta",
        metadata_denv2 = "terra/metadata_denv2.tsv",
        sequences_denv2 = "terra/sequences_denv2.fasta",
        metadata_denv3 = "terra/metadata_denv3.tsv",
        sequences_denv3 = "terra/sequences_denv3.fasta",
        metadata_denv4 = "terra/metadata_denv4.tsv",
        sequences_denv4 = "terra/sequences_denv4.fasta",
    shell:
        """
        tsv-filter -H --str-eq serotype:denv1 {input.metadata} > {output.metadata_denv1}
        tsv-filter -H --str-eq serotype:denv2 {input.metadata} > {output.metadata_denv2}
        tsv-filter -H --str-eq serotype:denv3 {input.metadata} > {output.metadata_denv3}
        tsv-filter -H --str-eq serotype:denv4 {input.metadata} > {output.metadata_denv4}
        
        tsv-select -H -f accession {output.metadata_denv1} | grep -v "accession" > terra/denv1.ids
        tsv-select -H -f accession {output.metadata_denv2} | grep -v "accession" > terra/denv2.ids
        tsv-select -H -f accession {output.metadata_denv3} | grep -v "accession" > terra/denv3.ids
        tsv-select -H -f accession {output.metadata_denv4} | grep -v "accession" > terra/denv4.ids
        
        smof grep -f terra/denv1.ids {input.sequences} > {output.sequences_denv1}
        smof grep -f terra/denv2.ids {input.sequences} > {output.sequences_denv2}
        smof grep -f terra/denv3.ids {input.sequences} > {output.sequences_denv3}
        smof grep -f terra/denv4.ids {input.sequences} > {output.sequences_denv4}
        """
