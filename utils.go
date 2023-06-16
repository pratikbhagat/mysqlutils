package mysqlutils

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

var DB_CONN *sql.DB

// Select executes a SELECT query on the specified table using the provided database connection.
// It returns the result as a slice of maps, where each map represents a row with column names as keys.

func Select(db *sql.DB, tableName string, columns []string, whereClause map[string]interface{}) (string, []map[string]interface{}, error) {
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + tableName

	// Prepare the WHERE clause if it exists
	var whereValues []interface{}
	if len(whereClause) > 0 {
		whereConditions := []string{}
		for key, value := range whereClause {
			whereConditions = append(whereConditions, fmt.Sprintf("%s = ?", key))
			whereValues = append(whereValues, value)
		}
		query += " WHERE " + strings.Join(whereConditions, " AND ")
	}

	rows, err := db.Query(query, whereValues...)
	if err != nil {
		return query, nil, err
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return query, nil, err
	}

	result := []map[string]interface{}{}

	for rows.Next() {
		columnPointers := make([]interface{}, len(columnNames))
		columnValues := make([]interface{}, len(columnNames))

		for i := range columnValues {
			columnPointers[i] = &columnValues[i]
		}

		err := rows.Scan(columnPointers...)
		if err != nil {
			return query, nil, err
		}

		rowData := make(map[string]interface{})
		for i, name := range columnNames {
			switch v := columnValues[i].(type) {
			case []byte:
				rowData[name] = string(v)
			default:
				rowData[name] = v
			}
		}

		result = append(result, rowData)
	}

	if err := rows.Err(); err != nil {
		return query, nil, err
	}

	return query, result, nil
}

// Insert inserts multiple rows into a table.
func Insert(db *sql.DB, tableName string, data []map[string]interface{}) (string, error) {
	var query = ``
	if len(data) == 0 {
		return query, nil // Nothing to insert
	}

	columns := make([]string, 0, len(data[0]))
	for key := range data[0] {
		columns = append(columns, key)
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	var values []interface{}
	query = fmt.Sprintf("INSERT INTO %s (%s) VALUES", tableName, strings.Join(columns, ", "))

	rowsValues := make([]string, 0, len(data))
	for _, row := range data {
		rowValues := make([]string, len(columns))
		for i, col := range columns {
			values = append(values, row[col])
			rowValues[i] = "?"
		}
		rowsValues = append(rowsValues, fmt.Sprintf("(%s)", strings.Join(rowValues, ", ")))
	}

	query += strings.Join(rowsValues, ", ")

	_, err := db.Exec(query, values...)
	if err != nil {
		return query, err
	}

	return query, nil
}

// Update updates multiple rows in a table based on the provided data and WHERE conditions.
func Update(db *sql.DB, table string, data map[string]interface{}, where []map[string]interface{}) (string, error) {
	query := "UPDATE %s SET "

	keys := []string{}
	values := []interface{}{}
	for key, value := range data {
		keys = append(keys, fmt.Sprintf("%s = ?", key))
		values = append(values, value)
	}
	query = fmt.Sprintf(query+strings.Join(keys, ", "), table)

	whereConditions := []string{}
	for _, condition := range where {
		for key, value := range condition {
			whereConditions = append(whereConditions, fmt.Sprintf("%s = ?", key))
			values = append(values, value)
		}
	}
	query += " WHERE " + strings.Join(whereConditions, " AND ")

	stmt, err := db.Prepare(query)
	if err != nil {
		return query, err
	}
	defer stmt.Close()
	_, err = stmt.Exec(values...)
	return query, err
}

func Delete(db *sql.DB, table string, conditions map[string]interface{}) (string, bool, error) {
	var query strings.Builder
	var args []interface{}

	query.WriteString("DELETE FROM " + table)

	if len(conditions) > 0 {
		query.WriteString(" WHERE ")

		// Build the conditions and collect the arguments
		i := 0
		for field, value := range conditions {
			if i > 0 {
				query.WriteString(" AND ")
			}
			query.WriteString(field + " = ?")
			args = append(args, value)
			i++
		}
	}

	// Execute the delete query
	result, err := db.Exec(query.String(), args...)
	if err != nil {
		return query.String(), false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return query.String(), false, err
	}
	return query.String(), rowsAffected > 0, nil
}
