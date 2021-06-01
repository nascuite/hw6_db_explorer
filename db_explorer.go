package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type handler struct {
	DB *sql.DB
}

type columnTable struct {
	fieldCol      string
	typeCol       string
	collationCol  sql.NullString
	nullCol       string
	keyCol        sql.NullString
	defaultCol    sql.NullString
	extraCol      sql.NullString
	privilegesCol string
	commentCol    sql.NullString
}

type queryParametrs struct {
	tablesFromBD          map[string][]columnTable
	queryRequestParametrs requestParametrs
}

type requestParametrs struct {
	table  *string
	id     *int
	offset int
	limit  int
}

type response map[string]interface{}

func prepareQueryValue(qp *queryParametrs, column string, value interface{}, isInsert bool) (*string, error) {
	var preparedString string

	tableColumns, ok := qp.tablesFromBD[*qp.queryRequestParametrs.table]
	if !ok {
		return nil, fmt.Errorf("query table is nil")
	}

	for _, col := range tableColumns {
		if col.fieldCol == column {
			if value == nil {
				if col.nullCol == "YES" {
					preparedString = "NULL"
					return &preparedString, nil
				} else {
					return nil, fmt.Errorf("field %v have invalid type", col.fieldCol)
				}
			}

			if col.keyCol.Valid {
				if col.keyCol.String == "PRI" {
					if isInsert {
						preparedString = ""
						return &preparedString, nil
					} else {
						return nil, fmt.Errorf("field %v have invalid type", col.fieldCol)
					}
				}
			}

			var typeOk bool

			switch value.(type) {
			case int:
				if col.typeCol == "int" {
					typeOk = true
					preparedString = strconv.Itoa(value.(int))
				}
			case float64:
				if col.typeCol == "int" {
					typeOk = true
					preparedString = strconv.FormatFloat(value.(float64), 'f', -1, 64)
				}
			case string:
				if col.typeCol == "varchar(255)" || col.typeCol == "text" {
					typeOk = true
					preparedString = "'" + value.(string) + "'"
				}
			}

			if !typeOk {
				return nil, fmt.Errorf("field %v have invalid type", col.fieldCol)
			}

			break
		}
	}

	return &preparedString, nil
}

func findPkQueryTable(tableName string, tablesBD map[string][]columnTable) string {
	var pkCol string

	columns, exists := tablesBD[tableName]

	if exists {
		for _, tableCol := range columns {
			if tableCol.keyCol.Valid {
				if tableCol.keyCol.String == "PRI" {
					pkCol = tableCol.fieldCol
					break
				}
			}
		}
	}

	return pkCol
}

func writeResponse(w http.ResponseWriter, httpStatus int, resp interface{}, err error) {
	var result []byte
	var errJson error
	status := http.StatusOK

	if httpStatus > 0 {
		status = httpStatus
	}

	if err != nil {
		result = []byte(`{"error": "` + err.Error() + `"}`)
	} else {
		result, errJson = json.Marshal(resp)
		if errJson != nil {
			status = http.StatusInternalServerError
			result = []byte(`{"error": "jsonMarshal ` + errJson.Error() + `"}`)
		}
	}

	w.WriteHeader(status)
	w.Write(result)
}

func (h *handler) getQueryParametrs(r *http.Request) (*queryParametrs, error, int) {
	var err error

	queryInfo := queryParametrs{}

	queryInfo.queryRequestParametrs, err = getReqParams(r)
	if err != nil {
		return nil, err, http.StatusInternalServerError
	}

	queryInfo.tablesFromBD, err = getBDinfo(h.DB)
	if err != nil {
		return nil, err, http.StatusInternalServerError
	}

	if queryInfo.queryRequestParametrs.table != nil {
		_, tableExist := queryInfo.tablesFromBD[*queryInfo.queryRequestParametrs.table]

		if !tableExist {
			return &queryInfo, fmt.Errorf("unknown table"), http.StatusNotFound
		}
	}

	return &queryInfo, nil, http.StatusOK
}

func getReqParams(r *http.Request) (requestParametrs, error) {
	var par requestParametrs

	for i, u := range strings.Split(r.URL.Path, "/") {
		if i == 1 && len(u) > 0 {
			table := u
			par.table = &table
		} else if i == 2 && len(u) > 0 {
			id, err := strconv.Atoi(u)
			if err != nil {
				return par, err
			}

			par.id = &id
		}
	}

	limit := r.URL.Query().Get("limit")
	if limit != "" {
		var err error

		par.limit, err = strconv.Atoi(limit)
		if err != nil {
			return par, err
		}
	} else {
		par.limit = 5
	}

	offset := r.URL.Query().Get("offset")
	if offset != "" {
		var err error

		par.offset, err = strconv.Atoi(offset)
		if err != nil {
			return par, err
		}
	} else {
		par.offset = 0
	}

	return par, nil
}

func getBDinfo(db *sql.DB) (map[string][]columnTable, error) {
	tablesFromDB := map[string][]columnTable{}
	columnsFromDB := make([]columnTable, 0)

	// Получим список таблиц схемы БД
	rows, err := db.Query("SHOW TABLES;")
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var tableName string

		err := rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}

		tablesFromDB[tableName] = columnsFromDB
	}

	rows.Close()

	for tableName := range tablesFromDB {
		query := fmt.Sprintf("SHOW FULL COLUMNS FROM `%s`;", tableName)

		rowsColumns, err := db.Query(query)
		if err != nil {
			return nil, err
		}

		columnsFromDB = make([]columnTable, 0)

		for rowsColumns.Next() {
			var columnDB columnTable

			err = rowsColumns.Scan(&columnDB.fieldCol,
				&columnDB.typeCol,
				&columnDB.collationCol,
				&columnDB.nullCol,
				&columnDB.keyCol,
				&columnDB.defaultCol,
				&columnDB.extraCol,
				&columnDB.privilegesCol,
				&columnDB.commentCol)
			if err != nil {
				rowsColumns.Close()
				return nil, err
			}

			columnsFromDB = append(columnsFromDB, columnDB)
		}

		rowsColumns.Close()

		tablesFromDB[tableName] = columnsFromDB
	}

	return tablesFromDB, nil
}

func selectData(qp *queryParametrs, db *sql.DB) (response, error, int) {
	var resp map[string]interface{}
	var queryBuilder strings.Builder

	if qp.queryRequestParametrs.table != nil {
		queryBuilder.WriteString("SELECT * ")
		queryBuilder.WriteString("FROM " + *qp.queryRequestParametrs.table)

		if qp.queryRequestParametrs.id != nil {
			columnPk := findPkQueryTable(*qp.queryRequestParametrs.table, qp.tablesFromBD)
			queryBuilder.WriteString(" WHERE ")
			queryBuilder.WriteString(columnPk)
			queryBuilder.WriteString(" = ?")
		}

		queryBuilder.WriteString(" limit " + strconv.Itoa(qp.queryRequestParametrs.limit))
		queryBuilder.WriteString(" offset " + strconv.Itoa(qp.queryRequestParametrs.offset))

		var err error
		var rows *sql.Rows

		if qp.queryRequestParametrs.id != nil {
			rows, err = db.Query(queryBuilder.String(), qp.queryRequestParametrs.id)
		} else {
			rows, err = db.Query(queryBuilder.String())
		}

		if err != nil {
			fmt.Println(queryBuilder.String())
			return nil, err, http.StatusInternalServerError
		}

		defer rows.Close()

		rowsColumns, err := rows.Columns()
		if err != nil {
			return nil, err, http.StatusInternalServerError
		}

		rowsColumnTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, err, http.StatusInternalServerError
		}

		var resultMap []map[string]interface{}
		var rowCount int

		for rows.Next() {
			rowCount++

			rowValues := make([]interface{}, len(rowsColumns))
			rowPointers := make([]interface{}, len(rowsColumns))

			for i, _ := range rowValues {
				rowPointers[i] = &rowValues[i]
			}

			err = rows.Scan(rowPointers...)
			if err != nil {
				return nil, err, http.StatusInternalServerError
			}

			rowMap := make(map[string]interface{})
			for i, val := range rowValues {
				valByte, ok := val.([]byte)

				var v interface{}

				if ok {

					switch rowsColumnTypes[i].DatabaseTypeName() {
					case "INT":
						v, _ = strconv.Atoi(string(valByte))
					default:
						v = string(valByte)
					}

				} else {
					v = val
				}

				rowMap[rowsColumns[i]] = v
			}

			resultMap = append(resultMap, rowMap)
		}

		if rowCount == 0 {
			return nil, fmt.Errorf("record not found"), http.StatusNotFound
		}

		if qp.queryRequestParametrs.id != nil && len(resultMap) > 0 {
			resp = response{"response": response{"record": resultMap[0]}}
		} else {
			resp = response{"response": response{"records": resultMap}}
		}
	}

	return resp, nil, http.StatusOK
}

func (qp *queryParametrs) updatePage(h *handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req := make(map[string]interface{})

		buf := new(bytes.Buffer)
		buf.ReadFrom(r.Body)

		err := json.Unmarshal(buf.Bytes(), &req)
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, nil, err)
			return
		}

		pkCol := findPkQueryTable(*qp.queryRequestParametrs.table, qp.tablesFromBD)

		var queryBuilder, columnBuilder strings.Builder
		var i int

		for columnName, columnValue := range req {
			queryValue, err := prepareQueryValue(qp, columnName, columnValue, false)
			if err != nil {
				writeResponse(w, http.StatusBadRequest, nil, err)
				return
			}

			if i > 0 {
				columnBuilder.WriteString(",")
			}

			columnBuilder.WriteString(columnName + " = " + *queryValue)

			i++
		}

		queryBuilder.WriteString("UPDATE ")
		queryBuilder.WriteString(*qp.queryRequestParametrs.table)
		queryBuilder.WriteString(" SET ")
		queryBuilder.WriteString(columnBuilder.String())
		queryBuilder.WriteString(" ")
		queryBuilder.WriteString(" WHERE " + pkCol + " = ?;")

		fmt.Println(queryBuilder.String())

		resultSql, err := h.DB.Exec(queryBuilder.String(), *qp.queryRequestParametrs.id)
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, nil, err)
			return
		}

		rows, err := resultSql.RowsAffected()
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, nil, err)
			return
		}

		result := response{"response": response{"updated": rows}}

		writeResponse(w, http.StatusOK, result, nil)
	}
}

func (qp *queryParametrs) insertPage(h *handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req := make(map[string]interface{})

		buf := new(bytes.Buffer)
		buf.ReadFrom(r.Body)

		err := json.Unmarshal(buf.Bytes(), &req)
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, nil, err)
			return
		}

		var queryBuilder, columnBuilder, valueBuilder strings.Builder
		var i int

		for column, value := range req {
			insertValue, err := prepareQueryValue(qp, column, value, true)
			if err != nil {
				writeResponse(w, http.StatusInternalServerError, nil, err)
				return
			}

			if len(*insertValue) > 0 {
				if i > 0 {
					columnBuilder.WriteString(", ")
					valueBuilder.WriteString(", ")
				}

				columnBuilder.WriteString(column)
				valueBuilder.WriteString(*insertValue)

				i++
			}
		}

		queryBuilder.WriteString("INSERT INTO ")
		queryBuilder.WriteString(*qp.queryRequestParametrs.table)
		queryBuilder.WriteString(" (" + columnBuilder.String() + ") ")
		queryBuilder.WriteString(" VALUES ")
		queryBuilder.WriteString(" ( " + valueBuilder.String() + "); ")

		resultSql, err := h.DB.Exec(queryBuilder.String())
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, nil, err)
			return
		}

		lastID, err := resultSql.LastInsertId()
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, nil, err)
			return
		}

		result := response{"response": response{"id": lastID}}

		writeResponse(w, http.StatusOK, result, nil)
	}
}

func (qp *queryParametrs) listPage(h *handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var result response

		if qp.queryRequestParametrs.table != nil {
			var err error
			var status int
			result, err, status = selectData(qp, h.DB)
			if err != nil {
				writeResponse(w, status, result, err)
				return
			}

		} else {
			var tables []string

			for tableName := range qp.tablesFromBD {
				tables = append(tables, tableName)
			}

			sort.Slice(tables, func(i, j int) bool {
				return tables[i] < tables[j]
			})

			result = response{"response": response{"tables": tables}}
		}

		writeResponse(w, http.StatusOK, result, nil)
	}
}

func (h *handler) handler(w http.ResponseWriter, r *http.Request) {
	qp, err, status := h.getQueryParametrs(r)
	if err != nil {
		writeResponse(w, status, nil, err)
		return
	}

	if qp == nil {
		err := fmt.Errorf("queryParametrs is nil")
		writeResponse(w, http.StatusInternalServerError, nil, err)
		return
	}

	var handlerPage http.HandlerFunc

	switch r.Method {
	case "DELETE":
		handlerPage = qp.listPage(h)
	case "GET":
		handlerPage = qp.listPage(h)
	case "POST":
		handlerPage = qp.updatePage(h)
	case "PUT":
		handlerPage = qp.insertPage(h)
	}

	handlerPage(w, r)
}

//NewDbExplorer
func NewDbExplorer(db *sql.DB) (http.HandlerFunc, error) {
	handler := &handler{
		DB: db,
	}

	return handler.handler, nil
}
