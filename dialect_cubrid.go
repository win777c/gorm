package gorm

import (
	"crypto/sha1"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// var mysqlIndexRegex = regexp.MustCompile(`^(.+)\((\d+)\)$`)

type cubrid struct {
	commonDialect
}

func init() {
	RegisterDialect("cubrid", &cubrid{})
}

func (cubrid) GetName() string {
	return "cubrid"
}

func (cubrid) Quote(key string) string {
	return fmt.Sprintf("`%s`", key)
}

// Get Data Type for MySQL Dialect
func (s *cubrid) DataTypeOf(field *StructField) string {
	var dataValue, sqlType, size, additionalType = ParseFieldStructForDialectCUBRID(field, s)

	// MySQL allows only one auto increment column per table, and it must
	// be a KEY column.
	if _, ok := field.TagSettingsGet("AUTO_INCREMENT"); ok {
		if _, ok = field.TagSettingsGet("INDEX"); !ok && !field.IsPrimaryKey {
			field.TagSettingsDelete("AUTO_INCREMENT")
		}
	}

	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "boolean"
		case reflect.Int8:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "tinyint AUTO_INCREMENT"
			} else {
				sqlType = "tinyint"
			}
		case reflect.Int, reflect.Int16, reflect.Int32:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "int AUTO_INCREMENT"
			} else {
				sqlType = "int"
			}
		case reflect.Uint8:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "tinyint unsigned AUTO_INCREMENT"
			} else {
				sqlType = "tinyint unsigned"
			}
		case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "int unsigned AUTO_INCREMENT"
			} else {
				sqlType = "int unsigned"
			}
		case reflect.Int64:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "bigint AUTO_INCREMENT"
			} else {
				sqlType = "bigint"
			}
		case reflect.Uint64:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "bigint unsigned AUTO_INCREMENT"
			} else {
				sqlType = "bigint unsigned"
			}
		case reflect.Float32, reflect.Float64:
			sqlType = "double"
		case reflect.String:
			if size > 0 && size < 65532 {
				sqlType = fmt.Sprintf("varchar(%d)", size)
			} else {
				sqlType = "longtext"
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				precision := ""
				if p, ok := field.TagSettingsGet("PRECISION"); ok {
					precision = fmt.Sprintf("(%s)", p)
				}

				if _, ok := field.TagSettings["NOT NULL"]; ok || field.IsPrimaryKey {
					sqlType = fmt.Sprintf("DATETIME%v", precision)
				} else {
					sqlType = fmt.Sprintf("DATETIME%v NULL", precision)
				}
			}
		default:
			if IsByteArrayOrSlice(dataValue) {
				sqlType = fmt.Sprintf("bit varying(%d)", size)
			}
		}
	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) in field %s for cubrid", dataValue.Type().Name(), dataValue.Kind().String(), field.Name))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

func (s cubrid) RemoveIndex(tableName string, indexName string) error {
	_, err := s.db.Exec(fmt.Sprintf("DROP INDEX %v ON %v", indexName, s.Quote(tableName)))
	return err
}

func (s cubrid) ModifyColumn(tableName string, columnName string, typ string) error {
	_, err := s.db.Exec(fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", tableName, columnName, typ))
	return err
}

func (s cubrid) LimitAndOffsetSQL(limit, offset interface{}) (sql string, err error) {
	if limit != nil {
		parsedLimit, err := s.parseInt(limit)
		if err != nil {
			return "", err
		}
		if parsedLimit >= 0 {
			sql += fmt.Sprintf(" LIMIT %d", parsedLimit)

			if offset != nil {
				parsedOffset, err := s.parseInt(offset)
				if err != nil {
					return "", err
				}
				if parsedOffset >= 0 {
					sql += fmt.Sprintf(" OFFSET %d", parsedOffset)
				}
			}
		}
	}
	return
}

func (s cubrid) HasForeignKey(tableName string, foreignKeyName string) bool {
	var count int
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	sql := fmt.Sprintf("SELECT count(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='%s' AND TABLE_NAME='%s' AND CONSTRAINT_NAME='%s' AND CONSTRAINT_TYPE='FOREIGN KEY'", currentDatabase, tableName, foreignKeyName)
	s.db.QueryRow(sql).Scan(&count)
	return count > 0
}

func (s cubrid) HasTable(tableName string) bool {
	var count int

	sql := fmt.Sprintf("SELECT COUNT(*) FROM db_class WHERE class_name = '%s'", tableName)
	s.db.QueryRow(sql).Scan(&count)

	return count > 0
}

func (s cubrid) HasIndex(tableName string, indexName string) bool {
	var count int
	sql := fmt.Sprintf("SELECT COUNT(*) FROM db_index WHERE class_name = '%s' AND index_name = '%s'", tableName, indexName)
	s.db.QueryRow(sql).Scan(&count)
	return count > 0
}

func (s cubrid) HasColumn(tableName string, columnName string) bool {

	var count int
	sql := fmt.Sprintf("SELECT COUNT(*) FROM db_attribute WHERE class_name = '%s' AND attr_name = '%s'", tableName, columnName)
	s.db.QueryRow(sql).Scan(&count)
	return count > 0
}

func (s cubrid) CurrentDatabase() (name string) {
	s.db.QueryRow("SELECT DATABASE()").Scan(&name)
	return
}

func (cubrid) SelectFromDummyTable() string {
	return "FROM db_root"
}

func (s cubrid) BuildKeyName(kind, tableName string, fields ...string) string {
	keyName := s.commonDialect.BuildKeyName(kind, tableName, fields...)
	if utf8.RuneCountInString(keyName) <= 64 {
		return keyName
	}
	h := sha1.New()
	h.Write([]byte(keyName))
	bs := h.Sum(nil)

	// sha1 is 40 characters, keep first 24 characters of destination
	destRunes := []rune(keyNameRegex.ReplaceAllString(fields[0], "_"))
	if len(destRunes) > 24 {
		destRunes = destRunes[:24]
	}

	return fmt.Sprintf("%s%x", string(destRunes), bs)
}

// NormalizeIndexAndColumn returns index name and column name for specify an index prefix length if needed
func (cubrid) NormalizeIndexAndColumn(indexName, columnName string) (string, string) {
	submatch := mysqlIndexRegex.FindStringSubmatch(indexName)
	if len(submatch) != 3 {
		return indexName, columnName
	}
	indexName = submatch[1]
	columnName = fmt.Sprintf("%s(%s)", columnName, submatch[2])
	return indexName, columnName
}

func (cubrid) DefaultValueStr() string {
	return "VALUES()"
}

// ParseFieldStructForDialectCUBRID get field's sql data type CUBRID
var ParseFieldStructForDialectCUBRID = func(field *StructField, dialect Dialect) (fieldValue reflect.Value, sqlType string, size int, additionalType string) {
	// Get redirected field type
	var (
		reflectType = field.Struct.Type
		dataType, _ = field.TagSettingsGet("TYPE")
	)

	switch strings.ToLower(dataType) {
	default:
		if strings.Contains(strings.ToLower(dataType), "nvarchar") == true {
			dataType = strings.Replace(dataType, "nvarchar", "varchar", 1)
		} else if strings.Contains(strings.ToLower(dataType), "binary") == true {
			dataType = strings.Replace(dataType, "binary", "bit varying", 1)
		}
	}

	for reflectType.Kind() == reflect.Ptr {
		reflectType = reflectType.Elem()
	}

	// Get redirected field value
	fieldValue = reflect.Indirect(reflect.New(reflectType))

	if gormDataType, ok := fieldValue.Interface().(interface {
		GormDataType(Dialect) string
	}); ok {
		dataType = gormDataType.GormDataType(dialect)
	}

	// Get scanner's real value
	if dataType == "" {
		var getScannerValue func(reflect.Value)
		getScannerValue = func(value reflect.Value) {
			fieldValue = value
			if _, isScanner := reflect.New(fieldValue.Type()).Interface().(sql.Scanner); isScanner && fieldValue.Kind() == reflect.Struct {
				getScannerValue(fieldValue.Field(0))
			}
		}
		getScannerValue(fieldValue)
	}

	// Default Size
	if num, ok := field.TagSettingsGet("SIZE"); ok {
		size, _ = strconv.Atoi(num)
	} else {
		size = 255
	}

	// Default type from tag setting
	notNull, _ := field.TagSettingsGet("NOT NULL")
	unique, _ := field.TagSettingsGet("UNIQUE")

	additionalType = notNull + " " + unique
	// Oarcle에서 Default 값이 앞에 와야 테이블이 생성됨
	if value, ok := field.TagSettingsGet("DEFAULT"); ok {
		additionalType = " DEFAULT " + value + " " + additionalType
	}

	if value, ok := field.TagSettingsGet("COMMENT"); ok {
		additionalType = additionalType + " COMMENT " + value
	}

	return fieldValue, dataType, size, strings.TrimSpace(additionalType)
}
