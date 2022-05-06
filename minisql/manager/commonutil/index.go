package index

//index类
type Index struct {
	IndexName     string
	TableName     string
	AttributeName string

	RootNum  int
	BlockNum int
}

func NewIndex(indexName string, tableName string, attributeName string) *Index {
	return &Index{IndexName: indexName, TableName: tableName, AttributeName: attributeName}
}

func NewIndex2(indexName string, tableName string, attributeName string, blockNum int, rootNum int) *Index {
	return &Index{IndexName: indexName, TableName: tableName, AttributeName: attributeName, BlockNum: blockNum, RootNum: rootNum}
}
