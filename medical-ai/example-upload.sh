#!/bin/bash
export TOKEN=$1
export MYFILE=$2

echo ${TOKEN} ${MYFILE}

export TMPJSON="/tmp/response-$(openssl rand -hex 4).json"
echo ${TMPJSON}

curl -X GET -H "Authorization: Bearer $TOKEN" -o ${TMPJSON} \
   https://medical-ai.sawtellelabs.com/mr-prostate-seg/api/v2/submit-job

cat ${TMPJSON}

export url=$(cat ${TMPJSON} | jq .url)
export key=$(cat ${TMPJSON} | jq .fields.key)
export policy=$(cat ${TMPJSON} | jq .fields.policy)
export algorithm=$(cat ${TMPJSON} | jq '.fields."x-amz-algorithm"')
export credential=$(cat ${TMPJSON} | jq '.fields."x-amz-credential"')
export date=$(cat ${TMPJSON} | jq '.fields."x-amz-date"')
export signature=$(cat ${TMPJSON} | jq '.fields."x-amz-signature"')
export usage_id=$(cat ${TMPJSON} | jq .usage_id)

echo curl -X POST \
    -F key="${key}" \
    -F policy="${policy}" \
    -F x-amz-algorithm="${algorithm}" \
    -F x-amz-credential="${credential}" \
    -F x-amz-date="${date}" \
    -F x-amz-signature="${signature}" \
    -F file="@$MYFILE" \
    "$url"

curl -X POST \
    -F key="${key}" \
    -F policy="${policy}" \
    -F x-amz-algorithm="${algorithm}" \
    -F x-amz-credential="${credential}" \
    -F x-amz-date="${date}" \
    -F x-amz-signature="${signature}" \
    -F file=@${MYFILE} \
    "${url}"

echo curl -X GET -H "Authorization: Bearer $TOKEN" \
    "https://medical-ai.sawtellelabs.com/mr-prostate-seg/api/v1/check-job-status?usage_id=${usage_id}"

curl -X GET -H "Authorization: Bearer $TOKEN" \
    "https://medical-ai.sawtellelabs.com/mr-prostate-seg/api/v1/check-job-status?usage_id=${usage_id}"

"""
export TOKEN=
export NIFTI_FILE=../../huggingface/mr-prostate-segmentation-model/files/test-Case00.nii.gz
bash example-upload.sh $TOKEN $NIFTI_FILE
"""