package indexmanager

import (
	"github.com/google/btree"

	catalogmanager "Distributed-MiniSQL/minisql/manager/catalogmanager"
)

// type Element struct {
// 	Data     interface{}
// 	ColumnId int
// 	Datatype string
// }

// type Row struct {
// 	Elements []Element
// }

type node struct {
	key   interface{} //Go语言中任何对象都满足空接口，因此可以看成是可以指定任何对象的any类型
	value catalogmanager.Address
}

// btree存放的东西必须实现Less(),即Item接口
func (i *node) Less(b btree.Item) bool {
	switch i.key.(type) {
	case int:
		return i.key.(int) < b.(*node).key.(int) //类型断言
	case float32:
		return i.key.(float32) < b.(*node).key.(float32)
	case float64:
		return i.key.(float64) < b.(*node).key.(float64)
	case string:
		return i.key.(string) < b.(*node).key.(string)
	}
	return false
}

type BPTree struct {
	tree *btree.BTree
}

func NewBPTree() *BPTree {
	return &BPTree{btree.New(16)}
}

func (b *BPTree) Insert(key interface{}, value catalogmanager.Address) { //插入元素
	b.tree.ReplaceOrInsert(&node{key: key, value: value})
}

func (b BPTree) FindEq(key interface{}) *catalogmanager.Address { //寻找是否有相等元素
	value := b.tree.Get(&node{key: key})
	if value == nil {
		return nil
	}
	f := value.(*node)
	return &f.value
}

func (b BPTree) FindNeq(key interface{}) []catalogmanager.Address {
	var value []catalogmanager.Address
	b.tree.Ascend(func(item btree.Item) bool {
		f := item.(*node)
		if f.key != key {
			value = append(value, f.value)
		}
		return true
	})
	return value
}

func (b BPTree) FindLess(key interface{}) []catalogmanager.Address {
	var value []catalogmanager.Address
	b.tree.AscendLessThan(&node{key: key}, func(item btree.Item) bool {
		f := item.(*node)
		value = append(value, f.value)
		return true
	})
	return value
}

func (b BPTree) FindLeq(key interface{}) []catalogmanager.Address {
	var value = b.FindLess(key)
	var value2 = b.FindEq(key)
	if value2 != nil {
		value = append(value, *value2)
	}
	return value
}

func (b BPTree) FindGeq(key interface{}) []catalogmanager.Address {
	var value []catalogmanager.Address
	b.tree.AscendGreaterOrEqual(&node{key: key}, func(item btree.Item) bool {
		f := item.(*node)
		value = append(value, f.value)
		return true
	})
	return value
}

func (b BPTree) FindGreater(key interface{}) []catalogmanager.Address {
	var value = b.FindGeq(key)
	if value != nil {
		if b.FindEq(key) != nil { //说明有相等的情况
			value = value[1:]
		}
		// if(value[0].Elements[0].Data == key){ //去除等于的情况
		// 	value = value[1:]
		// }
	}
	return value
}

func (b *BPTree) Delete(key interface{}) bool {
	var value = b.tree.Delete(&node{key: key})
	return value != nil
}

// func Main() {
// 	tree := NewBPTree()
// 	tree2 := NewBPTree()

// 	for i := 0; i < 1000; i++ {
// 		str := fmt.Sprintf("%d", i)
// 		str2 := fmt.Sprintf("%d#%d", i, i)
// 		var temp float64 = float64(i) + 0.1
// 		var element = Element{Data: temp, ColumnId: 1, Datatype: "float"}
// 		var element2 = Element{Data: str, ColumnId: 1, Datatype: "string"}
// 		var element3 = Element{Data: str2, ColumnId: 2, Datatype: "string"}
// 		// tree.insert(i, {element, element2})
// 		tree.Insert(temp, []Element{element, element2})
// 		tree2.Insert(str, []Element{element2, element3})
// 	}
// 	res1 := tree.FindEq(998.1);
// 	res2 := tree2.FindEq("999")
// 	if(res1 != nil){
// 		fmt.Println(res1.Elements)
// 	}
// 	if(res2 != nil){
// 		fmt.Println(res2.Elements)
// 	}
// 	// res3 := tree.FindLeq(30);
// 	// if(res3 != nil){
// 	// 	fmt.Println(res3)
// 	// }
// 	res4 := tree.FindGreater(990.1);
// 	if(res4 != nil){
// 		fmt.Println(res4)
// 	}

// 	var element2 = Element{Data: "998", ColumnId: 1, Datatype: "float"}
// 	var element3 = Element{Data: "test", ColumnId: 2, Datatype: "string"}
// 	tree2.Insert("998",[]Element{element2, element3})
// 	fmt.Println(tree2.Delete("999"))
// 	res4 = tree2.FindGreater("990");
// 	if(res4 != nil){
// 		fmt.Println(res4)
// 	}
// 	// //创建一棵btree
// 	// tree := NewBPTree();
// 	// tree2 := NewBPTree();

// 	// for i := 0; i < 1000; i++ {
// 	// 	// str2 := fmt.Sprintf("%d", i);
// 	// 	var newNode = node{key: i}
// 	// 	// fmt.Println(newNode.key)
// 	// 	// fmt.Println(newNode.value)
// 	// 	tree.ReplaceOrInsert(&newNode)
// 	// }

// 	// tree.AscendGreaterOrEqual(&node{key: 550}, func(item btree.Item) bool {
// 	// 	f := item.(*node)
// 	// 	fmt.Println(f.key)
// 	// 	fmt.Println(f.value)
// 	// 	return true
// 	// })
// }
