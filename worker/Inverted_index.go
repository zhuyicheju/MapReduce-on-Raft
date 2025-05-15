package worker

import (
	"sort"
	"strings"
	"unicode"
)

func Map(filename string, contents string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into words.
	words := strings.FieldsFunc(contents, ff)

	// 使用 map 去重，确保每个词在每个文档只记录一次
	uniqueWords := make(map[string]bool)
	for _, word := range words {
		if word != "" {
			uniqueWords[word] = true
		}
	}

	kva := []KeyValue{}
	for word := range uniqueWords {
		// Key 是词，Value 是文档名
		kva = append(kva, KeyValue{Key: word, Value: filename})
	}
	return kva
}

func Reduce(key string, values []string) string {
	// 使用 map 去重，确保文档名唯一
	docSet := make(map[string]bool)
	for _, doc := range values {
		docSet[doc] = true
	}

	// 将文档名转成 slice 并排序（可选，便于调试或比对）
	var docs []string
	for doc := range docSet {
		docs = append(docs, doc)
	}
	sort.Strings(docs)

	// 连接为逗号分隔的字符串
	return strings.Join(docs, ",")
}
