package info

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/beevik/etree"
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
	WildcardAlias     string
	wildcardTableInfo *TableInfo
}

type replaceProcessor struct {
	origin  string
	replace string
}

type stmtProcessState struct {
	// Fragments of the statement text.
	stmtFragments []string

	// --- The following are for SELECT only.

	// Position marker for wildcard expansion.
	wildcardMarker string

	// Fragments of the statement text for SELECT query to obtain result columns information.
	stmtFragments4Query []string

	// Wildcard table names and (optionally) their aliases.
	wildcardTableNames []string
	wildcardAliases    []string
}

func (p *charDataProcessor) Initialize(db *sql.DB, dbInfo *DBInfo, tok etree.Token) error {
	p.charData = tok.(*etree.CharData).Data
	return nil
}

func (p *charDataProcessor) GenStmtFragments(state *stmtProcessState) error {
	state.AddFragments([]string{p.charData})
	return nil
}

func (p *charDataProcessor) GenStmtFragments4Query(state *stmtProcessState) error {
	state.AddFragments4Query([]string{p.charData})
	return nil
}

func (p *wildcardProcessor) Initialize(db *sql.DB, dbInfo *DBInfo, tok etree.Token) error {
	panic("")
}

func (p *wildcardProcessor) GenStmtFragments(state *stmtProcessState) error {
	panic("")
}

func (p *wildcardProcessor) GenStmtFragments4Query(state *stmtProcessState) error {
	panic("")
}

func (p *replaceProcessor) Initialize(db *sql.DB, dbInfo *DBInfo, tok etree.Token) error {
	panic("")
}

func (p *replaceProcessor) GenStmtFragments(state *stmtProcessState) error {
	panic("")
}

func (p *replaceProcessor) GenStmtFragments4Query(state *stmtProcessState) error {
	panic("")
}

func newStmtProcessState() *stmtProcessState {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return &stmtProcessState{
		// Generate a random marker.
		wildcardMarker: hex.EncodeToString(buf),
	}
}

func (state *stmtProcessState) AddFragments(fragments []string) {
	state.stmtFragments = append(state.stmtFragments, fragments...)
}

func (state *stmtProcessState) AddFragments4Query(fragments []string) {
	state.stmtFragments4Query = append(state.stmtFragments4Query, fragments...)
}

func (state *stmtProcessState) AddWildcardFragments4Query(wildcardTableName, wildcardAlias string, fragments []string) {
	wcIdx := len(state.wildcardTableNames)
	state.stmtFragments4Query = append(state.stmtFragments4Query, fmt.Sprintf("1 AS %s_%s_b, ", state.wildcardMarker, wcIdx))
	state.stmtFragments4Query = append(state.stmtFragments4Query, fragments...)
	state.stmtFragments4Query = append(state.stmtFragments4Query, fmt.Sprintf(", 1 AS %s_%s_e", state.wildcardMarker, wcIdx))

	state.wildcardTableNames = append(state.wildcardTableNames, wildcardTableName)
	state.wildcardAliases = append(state.wildcardAliases, wildcardAlias)
}

var (
	processorFactory = map[string]func() stmtProcessor{}
)

func registProcessorFactory(factory func() stmtProcessor, tags ...string) {
	for _, tag := range tags {
		processorFactory[tag] = factory
	}
}

func processStmt(db *sql.DB, dbInfo *DBInfo, stmtElem *etree.Element, stmtInfo *StmtInfo) error {

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

	//state := newStmtProcessState()

	// TODO

	return nil
}
