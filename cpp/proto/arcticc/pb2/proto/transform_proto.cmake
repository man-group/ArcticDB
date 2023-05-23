FILE(READ "${IN_FILE}" infile)
STRING(REPLACE "import \"arcticc/pb2/" "import \"" infile_mod "${infile}")
FILE(WRITE "${OUT_FILE}" "${infile_mod}")
