package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
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

type tableInfo struct {
	name    string
	columns []columnTable
}

type queryParametrs struct {
	tablesFromBD          []tableInfo
	queryRequestParametrs requestParametrs
}

type requestParametrs struct {
	table  *string
	id     *string
	offset int
	limit  int
}

type response map[string]interface{}

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

func (h *handler) getSchema(r *http.Request) (*queryParametrs, error) {
	var err error

	queryInfo := queryParametrs{}

	queryInfo.queryRequestParametrs, err = getReqParams(r)
	if err != nil {
		return nil, err
	}

	queryInfo.tablesFromBD, err = getBDinfo(h.DB)
	if err != nil {
		return nil, err
	}

	return &queryInfo, nil
}

func getReqParams(r *http.Request) (requestParametrs, error) {
	var par requestParametrs

	for i, u := range strings.Split(r.URL.Path, "/") {
		if i == 1 && len(u) > 0 {
			par.table = &u
		} else if i == 2 && len(u) > 0 {
			par.id = &u
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

func getBDinfo(db *sql.DB) ([]tableInfo, error) {
	var tablesFromDB []tableInfo
	var columnsFromDB []columnTable

	// Получим список таблиц схемы БД
	rows, err := db.Query("SHOW TABLES;")
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		tableFromDB := tableInfo{}

		err := rows.Scan(&tableFromDB.name)
		if err != nil {
			return nil, err
		}

		rowsColumns, err := db.Query("SHOW FULL COLUMNS FROM ?;", tableFromDB.name)
		if err != nil {
			return nil, err
		}

		for rows.Next() {
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
				return nil, err
			}

			columnsFromDB = append(columnsFromDB, columnDB)
		}

		rowsColumns.Close()

		tableFromDB.columns = columnsFromDB

		tablesFromDB = append(tablesFromDB, tableFromDB)
	}

	rows.Close()

	return tablesFromDB, nil
}

func (sch *queryParametrs) listPage(h *handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var result response

		if sch == nil {
			err := fmt.Errorf("sch queryParametrs is nil")
			writeResponse(w, http.StatusInternalServerError, nil, err)
			return
		}

		if sch.queryRequestParametrs.table != nil {
			var queryTbl string

			paramsTable := *sch.queryRequestParametrs.table

			for _, t := range sch.tablesFromBD {
				if paramsTable == t.name {
					queryTbl = t.name
					break
				}
			}

			if len(queryTbl) < 1 {
				err := fmt.Errorf("unknown table")
				writeResponse(w, http.StatusNotFound, nil, err)
				return
			} else {
				result = response{"response": "success"}
			}
		} else {
			var tables []string

			for _, t := range sch.tablesFromBD {
				tables = append(tables, t.name)
			}

			result = response{"response": response{"tables": tables}}
		}
		writeResponse(w, http.StatusOK, result, nil)
	}
}

func (h *handler) handler(w http.ResponseWriter, r *http.Request) {
	schema, err := h.getSchema(r)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, nil, err)
		return
	}

	//switch r.Method {
	//case "DELETE":
	//	http.HandleFunc("/", h.deletePage)
	//case "GET":
	//	http.HandleFunc("/", h.listPage)
	//case "POST":
	//	http.HandleFunc("/", h.updatePage)
	//case "PUT":
	//	http.HandleFunc("/", h.insertPage)
	//}
	//var f =  h.test()
	listPage := schema.listPage(h)
	listPage(w, r)
}

//NewDbExplorer
func NewDbExplorer(db *sql.DB) (http.HandlerFunc, error) {
	handler := &handler{
		DB: db,
	}

	return handler.handler, nil
}
