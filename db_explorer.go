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

type preparedQuery struct {
	value    *string
	required bool
}

type response map[string]interface{}

func prepareQueryValue(columnInfo columnTable, reqValues map[string]interface{}, isInsert bool) (preparedQuery, error) {
	var preparedString string
	var columnExists bool
	var reqValue interface{}

	for column, value := range reqValues {
		if column == columnInfo.fieldCol {
			columnExists = true
			reqValue = value
			break
		}
	}

	if columnExists {
		if reqValue == nil {
			if columnInfo.nullCol == "YES" {
				return preparedQuery{nil, true}, nil
			} else {
				return preparedQuery{nil, false},
					fmt.Errorf("field %v have invalid type", columnInfo.fieldCol)
			}
		}

		if columnInfo.keyCol.Valid {
			if columnInfo.keyCol.String == "PRI" {
				if isInsert {
					return preparedQuery{nil, false}, nil
				} else {
					return preparedQuery{nil, false},
						fmt.Errorf("field %v have invalid type", columnInfo.fieldCol)
				}
			}
		}

		var typeOk bool

		switch reqValue.(type) {
		case int:
			if columnInfo.typeCol == "int" {
				typeOk = true
				preparedString = strconv.Itoa(reqValue.(int))
			}
		case float64:
			if columnInfo.typeCol == "int" {
				typeOk = true
				preparedString = strconv.FormatFloat(reqValue.(float64), 'f', -1, 64)
			}
		case string:
			if columnInfo.typeCol == "varchar(255)" || columnInfo.typeCol == "text" {
				typeOk = true
				preparedString = reqValue.(string)
			}
		}

		if !typeOk {
			return preparedQuery{nil, false},
				fmt.Errorf("field %v have invalid type", columnInfo.fieldCol)
		}

		return preparedQuery{&preparedString, true}, nil
	} else {
		if isInsert {
			if columnInfo.nullCol != "YES" {
				switch columnInfo.typeCol {
				case "varchar(255)":
					preparedString = ""
				case "text":
					preparedString = ""
				case "int":
					preparedString = "0"
				}

				return preparedQuery{&preparedString, true}, nil
			}
		}

		return preparedQuery{nil, false}, nil
	}
}

func getPkQueryTable(tableName string, tablesBD map[string][]columnTable) string {
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

	var err error

	par.limit, err = strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil {
		par.limit = 5
	}

	par.offset, err = strconv.Atoi(r.URL.Query().Get("offset"))
	if err != nil {
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
			columnPk := getPkQueryTable(*qp.queryRequestParametrs.table, qp.tablesFromBD)
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

			for i := range rowValues {
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

func (qp *queryParametrs) deletePage(h *handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		colPk := getPkQueryTable(*qp.queryRequestParametrs.table, qp.tablesFromBD)

		var queryBuilder strings.Builder

		queryBuilder.WriteString("DELETE FROM ")
		queryBuilder.WriteString(*qp.queryRequestParametrs.table)
		queryBuilder.WriteString(" WHERE ")
		queryBuilder.WriteString(colPk + " = ?")

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

		result := response{"response": response{"deleted": rows}}

		writeResponse(w, http.StatusOK, result, nil)
	}
}

func (qp *queryParametrs) updatePage(h *handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		request := make(map[string]interface{})
		buf := new(bytes.Buffer)

		buf.ReadFrom(r.Body)

		err := json.Unmarshal(buf.Bytes(), &request)
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, nil, err)
			return
		}

		pkCol := getPkQueryTable(*qp.queryRequestParametrs.table, qp.tablesFromBD)

		var queryBuilder, columnBuilder strings.Builder
		var i int
		var values []interface{}

		tableColumns, ok := qp.tablesFromBD[*qp.queryRequestParametrs.table]
		if !ok {
			writeResponse(w, http.StatusInternalServerError, nil, fmt.Errorf("query table is nil"))
			return
		}

		for _, col := range tableColumns {
			queryValue, err := prepareQueryValue(col, request, false)
			if err != nil {
				writeResponse(w, http.StatusBadRequest, nil, err)
				return
			}

			if queryValue.required {
				if i > 0 {
					columnBuilder.WriteString(",")
				}

				columnBuilder.WriteString(col.fieldCol + " = ?")

				if queryValue.value != nil {
					values = append(values, *queryValue.value)
				} else {
					values = append(values, nil)
				}

				i++
			}
		}

		queryBuilder.WriteString("UPDATE ")
		queryBuilder.WriteString(*qp.queryRequestParametrs.table)
		queryBuilder.WriteString(" SET ")
		queryBuilder.WriteString(columnBuilder.String())
		queryBuilder.WriteString(" WHERE " + pkCol + " = ?;")

		values = append(values, *qp.queryRequestParametrs.id)

		resultSql, err := h.DB.Exec(queryBuilder.String(), values...)
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
		var values []interface{}

		tableColumns, ok := qp.tablesFromBD[*qp.queryRequestParametrs.table]
		if !ok {
			writeResponse(w, http.StatusInternalServerError, nil, fmt.Errorf("query table is nil"))
			return
		}

		for _, col := range tableColumns {
			insertValue, err := prepareQueryValue(col, req, true)
			if err != nil {
				writeResponse(w, http.StatusInternalServerError, nil, err)
				return
			}

			if insertValue.value != nil {
				if i > 0 {
					columnBuilder.WriteString(", ")
					valueBuilder.WriteString(", ")
				}

				columnBuilder.WriteString(col.fieldCol)
				valueBuilder.WriteString("?")

				values = append(values, *insertValue.value)

				i++
			}
		}

		queryBuilder.WriteString("INSERT INTO ")
		queryBuilder.WriteString(*qp.queryRequestParametrs.table)
		queryBuilder.WriteString(" (" + columnBuilder.String() + ") ")
		queryBuilder.WriteString(" VALUES ")
		queryBuilder.WriteString(" ( " + valueBuilder.String() + "); ")

		resultSql, err := h.DB.Exec(queryBuilder.String(), values...)
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, nil, err)
			return
		}

		lastID, err := resultSql.LastInsertId()
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, nil, err)
			return
		}

		idCol := getPkQueryTable(*qp.queryRequestParametrs.table, qp.tablesFromBD)
		result := response{"response": response{idCol: lastID}}

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
		writeResponse(w, http.StatusInternalServerError, nil, fmt.Errorf("queryParametrs is nil"))
		return
	}

	var handlerPage http.HandlerFunc

	switch r.Method {
	case "DELETE":
		handlerPage = qp.deletePage(h)
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
