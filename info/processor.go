package info

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/beevik/etree"
	"strconv"
	"strings"
)

type stmtProcessor interface {
	// Initialize the stmtProcessor.
	Initialize(db *sql.DB, dbInfo *DBInfo, tok etree.Token) error

	// GenStmtFragments() generate statement text.
	GenStmtFragments(state *stmtProcessState) error

	// (optional) GenStmtFragments4Query() generate statement text for SELECT query. Return an error if the processor does not support this.
	GenStmtFragments4Query(state *stmtProcessState) error
}

type charDataProcessor struct {
	charData string
}

type wildcardProcessor struct {
	wildcardTableName string
	wildcardAlias     string
	wildcardTableInfo *TableInfo
}

type replaceProcessor struct {
	origin  string
	replace string
}

type stmtProcessState struct {
	// --- Query phase variables.

	// Position marker for wildcard expansion.
	wildcardMarker string

	// Fragments of the statement text for SELECT query to obtain result columns information.
	stmtFragments4Query []string

	// Wildcard table names and (optionally) their aliases.
	wildcardTableNames []string
	wildcardAliases    []string

	// --- Statement text phase variables.

	// Fragments of the statement text.
	stmtFragments []string
}

var (
	processorFactory = map[string]func() stmtProcessor{
		"wildcard": func() stmtProcessor { return &wildcardProcessor{} },
		"replace":  func() stmtProcessor { return &replaceProcessor{} },
	}
)

func (p *charDataProcessor) Initialize(db *sql.DB, dbInfo *DBInfo, tok etree.Token) error {
	p.charData = tok.(*etree.CharData).Data
	return nil
}

func (p *charDataProcessor) GenStmtFragments(state *stmtProcessState) error {
	state.AddFragments([]string{p.charData})
	return nil
}

func (p *charDataProcessor) GenStmtFragments4Query(state *stmtProcessState) error {
	state.AddFragments([]string{p.charData})
	return nil
}

func (p *wildcardProcessor) Initialize(db *sql.DB, dbInfo *DBInfo, tok etree.Token) error {

	elem := tok.(*etree.Element)

	wildcardTableName := elem.SelectAttrValue("table", "")
	if wildcardTableName == "" {
		return fmt.Errorf("Missing 'table' attribute in <wildcard>")
	}

	wildcardAlias := elem.SelectAttrValue("as", "")

	wildcardTableInfo, found := dbInfo.TableByName(wildcardTableName)
	if !found {
		return fmt.Errorf("Table %+q not found", wildcardTableName)
	}

	p.wildcardTableName = wildcardTableName
	p.wildcardAlias = wildcardAlias
	p.wildcardTableInfo = wildcardTableInfo
	return nil

}

func (p *wildcardProcessor) expansion() []string {

	prefix := p.wildcardAlias
	if prefix == "" {
		prefix = p.wildcardTableName
	}

	ret := []string{}
	for i := 0; i < p.wildcardTableInfo.NumColumn(); i++ {
		if i != 0 {
			ret = append(ret, ", ")
		}
		column := p.wildcardTableInfo.Column(i)
		ret = append(ret, fmt.Sprintf("%s.%s", prefix, column.ColumnName()))
	}

	return ret

}

func (p *wildcardProcessor) GenStmtFragments(state *stmtProcessState) error {
	state.AddFragments(p.expansion())
	return nil
}

func (p *wildcardProcessor) GenStmtFragments4Query(state *stmtProcessState) error {
	state.AddWildcardFragments4Query(p.wildcardTableName, p.wildcardAlias, p.expansion())
	return nil
}

func (p *replaceProcessor) Initialize(db *sql.DB, dbInfo *DBInfo, tok etree.Token) error {

	elem := tok.(*etree.Element)

	replace := elem.SelectAttrValue("with", "")
	if replace == "" {
		return fmt.Errorf("Missing 'with' attribute in <replace>")
	}

	p.replace = replace
	p.origin = elem.Text()

	return nil

}

func (p *replaceProcessor) GenStmtFragments(state *stmtProcessState) error {
	state.AddFragments([]string{p.origin})
	return nil
}

func (p *replaceProcessor) GenStmtFragments4Query(state *stmtProcessState) error {
	state.AddFragments4Query([]string{p.replace})
	return nil
}

func (state *stmtProcessState) AddFragments(fragments []string) {
	state.stmtFragments = append(state.stmtFragments, fragments...)
}

func (state *stmtProcessState) AddFragments4Query(fragments []string) {
	state.stmtFragments4Query = append(state.stmtFragments4Query, fragments...)
}

func (state *stmtProcessState) AddWildcardFragments4Query(wildcardTableName, wildcardAlias string, fragments []string) {

	wcIdx := len(state.wildcardTableNames)
	state.stmtFragments4Query = append(state.stmtFragments4Query, fmt.Sprintf("1 AS %s_%d_b, ", state.wildcardMarker, wcIdx))
	state.stmtFragments4Query = append(state.stmtFragments4Query, fragments...)
	state.stmtFragments4Query = append(state.stmtFragments4Query, fmt.Sprintf(", 1 AS %s_%d_e", state.wildcardMarker, wcIdx))

	state.wildcardTableNames = append(state.wildcardTableNames, wildcardTableName)
	state.wildcardAliases = append(state.wildcardAliases, wildcardAlias)

}

func (state *stmtProcessState) parseWildcardMarker(name string) (isMarker bool, isBegin bool, wildcardIdx int) {

	parts := strings.Split(name, "_")
	if len(parts) != 3 || parts[0] != state.wildcardMarker {
		return false, false, 0
	}

	// Check idx part.
	i, err := strconv.Atoi(parts[1])
	if err != nil {
		panic(fmt.Errorf("Invalid marker format %+q %+q", name, state.wildcardMarker))
	}

	// Check suffix part.
	switch parts[2] {
	case "b", "e":
	default:
		panic(fmt.Errorf("Invalid marker format %+q %+q", name, state.wildcardMarker))
	}

	return true, parts[2] == "b", i
}

func (state *stmtProcessState) stmtText4Query() string {
	return strings.Join(state.stmtFragments4Query, "")
}

func (state *stmtProcessState) stmtText() string {
	return strings.Join(state.stmtFragments, "")
}

func (state *stmtProcessState) process(db *sql.DB, dbInfo *DBInfo, stmtElem *etree.Element, stmtInfo *StmtInfo) error {

	// Reset
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	state.wildcardMarker = "wc" + hex.EncodeToString(buf)
	state.stmtFragments4Query = nil
	state.wildcardTableNames = nil
	state.wildcardAliases = nil
	state.stmtFragments = nil

	// Clear
	stmtInfo.resultColumnNames = nil
	stmtInfo.resultColumnTypes = nil
	stmtInfo.wildcardColumns = nil
	stmtInfo.wildcardAliases = nil

	// Convert xml to a list of stmtProcessor.
	ps := []stmtProcessor{}

	for _, t := range stmtElem.Child {

		p := stmtProcessor(nil)

		switch tok := t.(type) {

		case *etree.CharData:
			p = &charDataProcessor{}

		case *etree.Element:
			factory := processorFactory[tok.Tag]
			if factory == nil {
				return fmt.Errorf("Unknown processor <%s>", tok.Tag)
			}
			p = factory()

		}

		if err := p.Initialize(db, dbInfo, t); err != nil {
			return err
		}

		ps = append(ps, p)

	}

	if stmtInfo.StmtType() == "SELECT" {

		// Construct statement text for query.
		for _, p := range ps {
			if err := p.GenStmtFragments4Query(state); err != nil {
				return err
			}
		}
		stmtText4Query := state.stmtText4Query()

		// Query!
		rows, err := db.Query(stmtText4Query)
		if err != nil {
			return err
		}

		// Extract result column names and types.
		resultColumnNames, err := rows.Columns()
		if err != nil {
			return err
		}

		resultColumnTypes, err := rows.ColumnTypes()
		if err != nil {
			return err
		}

		// Wildcard mode variables.
		inWc := false
		wcTableInfo := (*TableInfo)(nil)
		wcAlias := ""
		wcColumnPos := 0

		for i, resultColumnName := range resultColumnNames {

			isMarker, isBegin, wildcardIdx := state.parseWildcardMarker(resultColumnName)
			if isMarker {
				// It's a marker column, toggle wildcard mode.
				if !inWc {
					if !isBegin {
						panic(fmt.Errorf("Expect begin marker but got end marker"))
					}

					// Enter wildcard mode.
					wcTableInfo = dbInfo.TableByNameM(state.wildcardTableNames[wildcardIdx])
					wcAlias = state.wildcardAliases[wildcardIdx]
					wcColumnPos = 0
					inWc = true

				} else {
					if isBegin {
						panic(fmt.Errorf("Expect end marker but got begin marker"))
					}

					// Exit wildcard mode.
					wcTableInfo = nil
					wcAlias = ""
					wcColumnPos = 0
					inWc = false
				}

				continue
			}

			stmtInfo.resultColumnNames = append(stmtInfo.resultColumnNames, resultColumnName)
			resultColumnType := resultColumnTypes[i]
			stmtInfo.resultColumnTypes = append(stmtInfo.resultColumnTypes, resultColumnType)

			if !inWc {
				// Not in wildcard mode.
				stmtInfo.wildcardColumns = append(stmtInfo.wildcardColumns, nil)
				stmtInfo.wildcardAliases = append(stmtInfo.wildcardAliases, "")
				continue
			}

			wildcardColumn := wcTableInfo.Column(wcColumnPos)
			wcColumnPos += 1
			if wildcardColumn.ColumnType().ScanType() != resultColumnType.ScanType() {
				panic(fmt.Errorf("Wildcard expansion column type mismatch"))
			}
			stmtInfo.wildcardColumns = append(stmtInfo.wildcardColumns, wildcardColumn)
			stmtInfo.wildcardAliases = append(stmtInfo.wildcardAliases, wcAlias)
		}

	}

	// Construct statement text.
	for _, p := range ps {
		if err := p.GenStmtFragments(state); err != nil {
			return err
		}
	}
	stmtInfo.stmtText = state.stmtText()

	return nil
}
