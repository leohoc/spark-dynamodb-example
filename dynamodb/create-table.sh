aws dynamodb create-table \
  --cli-input-json file://prophecyTable.json

aws dynamodb create-table \
  --cli-input-json file://prophecyWithoutIndexTable.json

aws dynamodb create-table \
  --cli-input-json file://prophecyByDateTable.json