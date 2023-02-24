#!/bin/bash

# Input: list of absolute path to proto files
# Output: modified proto files with slight import changes
#
# The modification only occurs if the input xxhsum does not
# match the last hash of the input used to generate the altered
# file.

# Using xxhsum as the hash function since it is a dependency
# of arctic native, so we're sure it's installed in the container

function alter(){
    in_f=$1
    out_f=$(basename $in_f)
    in_sum=$(/usr/local/bin/xxhsum $in_f | cut -d' ' -f 1)

    if [ -f $out_f.xxh ]
    then
        prev_sum=$(cat $out_f.xxh)
        [[ "$prev_sum" == "$in_sum" ]] && grep "$in_sum" $out_f > /dev/null && return 0
        echo "xxhsum mismatch for $in_f: previous=$prev_sum, current=$in_sum"
    fi

    echo "alter_proto.sh ${in_f} $(pwd)/$out_f"

    cat > $out_f << EOF
// ==================================================
//            Automatically generated file
//               DO NOT EDIT MANUALLY
// Input hash: $in_sum
// ==================================================

EOF

    sed 's|import \"arcticc/pb2/|import \"|' $in_f  >>  $out_f
    cat > $out_f.xxh << EOF
$in_sum
EOF
}

for f in $* ; do
alter $f
done

