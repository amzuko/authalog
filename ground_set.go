package authalog

type groundSet struct {
	items []int64
}

var isIn = ExternalRelation{
	head: Literal{
		Predicate: "in",
		Terms:     makeVars(2),
	},
	run: func(intern interner, terms []Term) ([][]Term, error) {
		if !terms[1].IsConstant {
			panic("in/2 should be syntactically gauranteed to only recieve constant sets")
		}

		s := intern.getSet(terms[1].Value)
		if terms[0].IsConstant {
			for _, v := range s.items {
				if v == terms[0].Value {
					return [][]Term{terms}, nil
				}
			}
			return [][]Term{}, nil
		}
		results := make([][]Term, len(s.items))
		for i, v := range s.items {
			results[i] = []Term{Term{IsConstant: true, Value: v}, terms[1]}
		}
		return results, nil
	},
}
