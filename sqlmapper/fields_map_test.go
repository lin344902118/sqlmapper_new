package sqlmapper

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

const (
	TABLE_NAME = "test"
)

var MysqlDbMgr MysqlDB

type MysqlDB struct {
	db *sql.DB
}

func Test_FieldsMap_GetTagIndex(t *testing.T) {
	Convey("test FieldsMap GetTagIndex", t, func() {
		test := &TestStruct{}
		fm, err := NewFieldsMap(TABLE_NAME, test)
		So(err, ShouldBeNil)
		Convey("should return err when field not in struct", func() {
			field := "work"
			index, err := fm.GetTagIndex(field)
			So(err, ShouldBeError)
			So(index, ShouldBeZeroValue)
		})
		Convey("should return nil", func() {
			field := "name"
			index, err := fm.GetTagIndex(field)
			So(err, ShouldBeNil)
			So(index, ShouldEqual, 0)
		})
	})
}

func Test_FieldsMap_SQLUpdateByFieldNameInDB(t *testing.T) {
	Convey("test SQLUpdateByFieldNameInDB", t, func() {
		// 先添加，在修改，然后删除
		test := &TestStruct{
			Name: "link",
			Age: 30,
			Sex: "male",
		}
		fm, err := NewFieldsMap(TABLE_NAME, test)
		So(err, ShouldBeNil)
		fm.SQLInsert(MysqlDbMgr.db)
		newTest := &TestStruct{
			Name: "link",
			Age: 30,
			Sex: "female",
		}
		fm, err = NewFieldsMap(TABLE_NAME, newTest)
		So(err, ShouldBeNil)
		rowAffected, err := fm.SQLUpdateByFieldNameInDB(MysqlDbMgr.db, "name")
		So(err, ShouldBeNil)
		So(rowAffected, ShouldEqual, 1)
		defer fm.SQLDeleteByFieldNameInDB(MysqlDbMgr.db, "name")
	})
}

func Test_FieldsMap_SQLDeleteByFieldNameInDB(t *testing.T) {
	Convey("test SQLDeleteByFieldNameInDB", t, func() {
		// 先添加，然后删除
		test := &TestStruct{
			Name: "link",
			Age: 30,
			Sex: "male",
		}
		fm, err := NewFieldsMap(TABLE_NAME, test)
		So(err, ShouldBeNil)
		fm.SQLInsert(MysqlDbMgr.db)
		rowaffected, err := fm.SQLDeleteByFieldNameInDB(MysqlDbMgr.db, "name")
		So(err, ShouldBeNil)
		So(rowaffected, ShouldEqual, 1)
	})
}

func init() {
	db, err := sql.Open("mysql", "lgh:linguohao@(192.168.50.129:3306)/test")
	if err != nil {
		fmt.Println("open mysql failed.err", err.Error())
		return
	}
	err = db.Ping()
	if err != nil {
		fmt.Println("ping mysql failed.err", err.Error())
	}
	//defer db.Close()
	MysqlDbMgr = MysqlDB{
		db: db,
	}
}
