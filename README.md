Genome Dataset Processing Assignment
Objective I: Processing Genome Dataset
Your task is to write a Spark program to process a genome dataset and calculate the global count of all side-by-side repetitions of identical symbols across all sequences.

Example:
Input:

sequence1 TTTCCACAAAA sequence2 AATTGGCC

Output:

(TTT, 1) (AAAA, 1) (CC, 2) (AA, 1) (TT, 1) (GG, 1)

Objective II: Extracting Non-Repeating Sequences
Given a FASTA dataset containing genome sequences, write a program that extracts the sequences where no consecutive characters are identical. If any sequence contains consecutive repeating symbols (e.g., AA, TTT), the sequence should be excluded from the output.

Example:
Input:

sequence1 TTTCCACAAAA sequence2 AATTGGCC sequence3 CTATGATC

Output:

(CTATGATC, 1)

Input: The dataset is loaded directly from the S3 path (s3a://retail-employees/data/final_genome.fasta).





log the execution time of your spark application
