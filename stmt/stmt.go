package stmt

type StmtInfo struct {
	directiveLocals map[interface{}]interface{}
}

func (info *StmtInfo) DirectiveLocals(key interface{}) interface{} {
	return info.directiveLocals[key]
}

func (info *StmtInfo) SetDirectiveLocals(key, val interface{}) {
	info.directiveLocals[key] = val
}
