package jstream

import (
	"bytes"
	"runtime/debug"
	"testing"
)

func mkReader(s string) *bytes.Reader { return bytes.NewReader([]byte(s)) }

func TestDecoderSimple(t *testing.T) {
	var (
		counter int
		mv      *MetaValue
		body    = `[{
	"bio": "bada bing bada boom",
	"id": 1,
	"name": "Charles",
	"falseVal": false
}]`
	)

	decoder := NewDecoder(mkReader(body), 1)

	for mv = range decoder.Stream() {
		counter++
		assertEqual(t, 1, len(mv.Keys))
		assertEqual(t, "", mv.Keys[0])
		result, ok := (mv.Value).(map[string]interface{})
		assertTrue(t, ok)

		assertNotNil(t, result["bio"])
		valStr, ok := (result["bio"]).(string)
		assertTrue(t, ok)
		assertEqual(t, "bada bing bada boom", valStr)

		assertNotNil(t, result["id"])
		valInt, ok := result["id"].(float64)
		assertTrue(t, ok)
		assertEqual(t, 1, int(valInt))

		assertNotNil(t, result["name"])
		valStr, ok = (result["name"]).(string)
		assertTrue(t, ok)
		assertEqual(t, "Charles", valStr)

		assertNotNil(t, result["falseVal"])
		valBool, ok := (result["falseVal"]).(bool)
		assertTrue(t, ok)
		assertFalse(t, valBool)
	}

	assertEqual(t, 1, counter)
	assertNil(t, decoder.Err())
}

func TestDecoderSimpleForMapMapArray(t *testing.T) {
	var (
		counter int
		mv      *MetaValue
		body    = `{
	"1787005804808765": {
		"fun1": [1, 2, 3],
		"fun2": [2, 3, 4],
		"fun3": [3, 4, 5]
	},
	"1786133652424674": {
		"fun4": [4, 5, 6],
		"fun5": [5, 6, 7]
	}
}`
	)

	decoder := NewDecoder(mkReader(body), 2)
	for mv = range decoder.Stream() {
		counter++
		assertEqual(t, 2, len(mv.Keys))
		result, ok := (mv.Value).([]interface{})
		assertTrue(t, ok)
		assertEqual(t, 3, len(result))
		for index, value := range result {
			assertEqual(t, index+counter, int(value.(float64)))
		}

		switch counter {
		case 1:
			assertEqual(t, "1787005804808765", mv.Keys[0])
			assertEqual(t, "fun1", mv.Keys[1])
		case 2:
			assertEqual(t, "1787005804808765", mv.Keys[0])
			assertEqual(t, "fun2", mv.Keys[1])
		case 3:
			assertEqual(t, "1787005804808765", mv.Keys[0])
			assertEqual(t, "fun3", mv.Keys[1])
		case 4:
			assertEqual(t, "1786133652424674", mv.Keys[0])
			assertEqual(t, "fun4", mv.Keys[1])
		case 5:
			assertEqual(t, "1786133652424674", mv.Keys[0])
			assertEqual(t, "fun5", mv.Keys[1])
		default:
			t.Errorf("counter value is wrong!")
		}
	}

	assertEqual(t, 5, counter)
	assertNil(t, decoder.Err())
}

func TestDecoderSimpleForMapArray(t *testing.T) {
	var (
		counter int
		mv      *MetaValue
		body    = `{
	"1787005804808765": [1, 2, 3],
	"1786133652424674": [2, 3, 4],
	"1778037433542921": [3, 4, 5],
	"1773651959798900": [4, 5, 6]
}`
	)

	decoder := NewDecoder(mkReader(body), 1)

	for mv = range decoder.Stream() {
		counter++
		assertEqual(t, 1, len(mv.Keys))
		result, ok := (mv.Value).([]interface{})
		assertTrue(t, ok)
		assertEqual(t, 3, len(result))
		for index, value := range result {
			assertEqual(t, index+counter, int(value.(float64)))
		}

		switch counter {
		case 1:
			assertEqual(t, "1787005804808765", mv.Keys[0])
		case 2:
			assertEqual(t, "1786133652424674", mv.Keys[0])
		case 3:
			assertEqual(t, "1778037433542921", mv.Keys[0])
		case 4:
			assertEqual(t, "1773651959798900", mv.Keys[0])
		default:
			t.Errorf("counter value is wrong!")
		}
	}

	assertEqual(t, 4, counter)
	assertNil(t, decoder.Err())
}

func TestDecoderSimpleForEmitKV(t *testing.T) {
	var (
		counter int
		mv      *MetaValue
		body    = `{
	"1787005804808765": {
		"fun1": [1, 2, 3],
		"fun2": [2, 3, 4],
		"fun3": [3, 4, 5]
	},
	"1786133652424674": {
		"fun4": [4, 5, 6],
		"fun5": [5, 6, 7]
	}
}`
	)

	decoder := NewDecoder(mkReader(body), 2).EmitKV()
	for mv = range decoder.Stream() {
		counter++
		assertEqual(t, 2, len(mv.Keys))
		uRet, ok := (mv.Value).(KV)
		assertTrue(t, ok)
		assertNotNil(t, uRet)
		result, ok := (uRet.Value).([]interface{})
		assertTrue(t, ok)
		assertEqual(t, 3, len(result))
		for index, value := range result {
			assertEqual(t, index+counter, int(value.(float64)))
		}

		switch counter {
		case 1:
			assertEqual(t, "1787005804808765", mv.Keys[0])
			assertEqual(t, "fun1", mv.Keys[1])
			assertEqual(t, "fun1", uRet.Key)
		case 2:
			assertEqual(t, "1787005804808765", mv.Keys[0])
			assertEqual(t, "fun2", mv.Keys[1])
			assertEqual(t, "fun2", uRet.Key)
		case 3:
			assertEqual(t, "1787005804808765", mv.Keys[0])
			assertEqual(t, "fun3", mv.Keys[1])
			assertEqual(t, "fun3", uRet.Key)
		case 4:
			assertEqual(t, "1786133652424674", mv.Keys[0])
			assertEqual(t, "fun4", mv.Keys[1])
			assertEqual(t, "fun4", uRet.Key)
		case 5:
			assertEqual(t, "1786133652424674", mv.Keys[0])
			assertEqual(t, "fun5", mv.Keys[1])
			assertEqual(t, "fun5", uRet.Key)
		default:
			t.Errorf("counter value is wrong!")
		}
	}

	assertEqual(t, 5, counter)
	assertNil(t, decoder.Err())
}

func TestDecoderSimpleForDepth3(t *testing.T) {
	var (
		counter int
		mv      *MetaValue
		body    = `{
	"1787005804808765": {
		"service1": {
			"fun1": [1, 2, 3],
			"fun2": [2, 3, 4]
		},
		"service2": {
			"fun1": [3, 4, 5],
			"fun2": [4, 5, 6]
		},
		"service3": {
			"fun1": [5, 6, 7],
			"fun2": [6, 7, 8]
		}
	},
	"1786133652424674": {
		"service": {
			"fun1": [7, 8, 9],
			"fun2": [8, 9, 10]
		},
		"service5": {
			"fun1": [9, 10, 11],
			"fun2": [10, 11, 12]
		}
	}
}`
	)

	decoder := NewDecoder(mkReader(body), 3)
	for mv = range decoder.Stream() {
		counter++
		assertEqual(t, 3, len(mv.Keys))
		result, ok := (mv.Value).([]interface{})
		assertTrue(t, ok)
		assertEqual(t, 3, len(result))
		for index, value := range result {
			assertEqual(t, index+counter, int(value.(float64)))
		}

		switch counter {
		case 1:
			assertEqual(t, "1787005804808765", mv.Keys[0])
			assertEqual(t, "service1", mv.Keys[1])
			assertEqual(t, "fun1", mv.Keys[2])
		case 2:
			assertEqual(t, "1787005804808765", mv.Keys[0])
			assertEqual(t, "service1", mv.Keys[1])
			assertEqual(t, "fun2", mv.Keys[2])
		case 3:
			assertEqual(t, "1787005804808765", mv.Keys[0])
			assertEqual(t, "service2", mv.Keys[1])
			assertEqual(t, "fun1", mv.Keys[2])
		case 4:
			assertEqual(t, "1787005804808765", mv.Keys[0])
			assertEqual(t, "service2", mv.Keys[1])
			assertEqual(t, "fun2", mv.Keys[2])
		case 5:
			assertEqual(t, "1787005804808765", mv.Keys[0])
			assertEqual(t, "service3", mv.Keys[1])
			assertEqual(t, "fun1", mv.Keys[2])
		case 6:
			assertEqual(t, "1787005804808765", mv.Keys[0])
			assertEqual(t, "service3", mv.Keys[1])
			assertEqual(t, "fun2", mv.Keys[2])
		case 7:
			assertEqual(t, "1786133652424674", mv.Keys[0])
			assertEqual(t, "service", mv.Keys[1])
			assertEqual(t, "fun1", mv.Keys[2])
		case 8:
			assertEqual(t, "1786133652424674", mv.Keys[0])
			assertEqual(t, "service", mv.Keys[1])
			assertEqual(t, "fun2", mv.Keys[2])
		case 9:
			assertEqual(t, "1786133652424674", mv.Keys[0])
			assertEqual(t, "service5", mv.Keys[1])
			assertEqual(t, "fun1", mv.Keys[2])
		case 10:
			assertEqual(t, "1786133652424674", mv.Keys[0])
			assertEqual(t, "service5", mv.Keys[1])
			assertEqual(t, "fun2", mv.Keys[2])
		default:
			t.Errorf("counter value is wrong!")
		}
	}

	assertEqual(t, 10, counter)
	assertNil(t, decoder.Err())
}

func TestDecoderNested(t *testing.T) {
	var (
		counter int
		mv      *MetaValue
		body    = `{
  "1": {
    "bio": "bada bing bada boom",
    "id": 0,
    "name": "Roberto",
    "nested1": {
      "bio": "utf16 surrogate (\ud834\udcb2)\n\u201cutf 8\u201d",
      "id": 1.5,
      "name": "Roberto*Maestro",
      "nested2": { "nested2arr": [0,1,2], "nested3": {
        "nested4": { "depth": "recursion" }}
			}
		}
  },
  "2": {
    "nullfield": null,
    "id": -2
  }
}`
	)

	decoder := NewDecoder(mkReader(body), 2)

	for mv = range decoder.Stream() {
		counter++
		t.Logf("depth=%d offset=%d len=%d (%v)", mv.Depth, mv.Offset, mv.Length, mv.Value)
	}

	if err := decoder.Err(); err != nil {
		t.Fatalf("decoder error: %s", err)
	}
}

func TestDecoderFlat(t *testing.T) {
	var (
		counter int
		mv      *MetaValue
		body    = `[
  "1st test string",
  "Roberto*Maestro", "Charles",
  0, null, false,
  1, 2.5
]`
		expected = []struct {
			Value     interface{}
			ValueType ValueType
		}{
			{
				"1st test string",
				String,
			},
			{
				"Roberto*Maestro",
				String,
			},
			{
				"Charles",
				String,
			},
			{
				int64(0),
				Number,
			},
			{
				nil,
				Null,
			},
			{
				false,
				Boolean,
			},
			{
				int64(1),
				Number,
			},
			{
				2.5,
				Number,
			},
		}
	)

	decoder := NewDecoder(mkReader(body), 1)

	for mv = range decoder.Stream() {
		if mv.Value != expected[counter].Value {
			t.Fatalf("got %v, expected: %v", mv.Value, expected[counter])
		}
		if mv.ValueType != expected[counter].ValueType {
			t.Fatalf("got %v value type, expected: %v value type", mv.ValueType, expected[counter].ValueType)
		}
		counter++
		t.Logf("depth=%d offset=%d len=%d (%v)", mv.Depth, mv.Offset, mv.Length, mv.Value)
	}

	if err := decoder.Err(); err != nil {
		t.Fatalf("decoder error: %s", err)
	}
}

func TestDecoderMultiDoc(t *testing.T) {
	var (
		counter int
		mv      *MetaValue
		body    = `{ "bio": "bada bing bada boom", "id": 1, "name": "Charles" }
{ "bio": "bada bing bada boom", "id": 2, "name": "Charles" }
{ "bio": "bada bing bada boom", "id": 3, "name": "Charles" }
{ "bio": "bada bing bada boom", "id": 4, "name": "Charles" }
{ "bio": "bada bing bada boom", "id": 5, "name": "Charles" }
`
	)

	decoder := NewDecoder(mkReader(body), 0)

	for mv = range decoder.Stream() {
		if mv.ValueType != Object {
			t.Fatalf("got %v value type, expected: Object value type", mv.ValueType)
		}
		counter++
		t.Logf("depth=%d offset=%d len=%d (%v)", mv.Depth, mv.Offset, mv.Length, mv.Value)
	}
	if err := decoder.Err(); err != nil {
		t.Fatalf("decoder error: %s", err)
	}
	if counter != 5 {
		t.Fatalf("expected 5 items, got %d", counter)
	}

	// test at depth level 1
	counter = 0
	kvcounter := 0
	decoder = NewDecoder(mkReader(body), 1)

	for mv = range decoder.Stream() {
		switch mv.Value.(type) {
		case KV:
			kvcounter++
		default:
			counter++
		}
		t.Logf("depth=%d offset=%d len=%d (%v)", mv.Depth, mv.Offset, mv.Length, mv.Value)
	}
	if err := decoder.Err(); err != nil {
		t.Fatalf("decoder error: %s", err)
	}
	if kvcounter != 0 {
		t.Fatalf("expected 0 keyvalue items, got %d", kvcounter)
	}
	if counter != 15 {
		t.Fatalf("expected 15 items, got %d", counter)
	}

	// test at depth level 1 w/ emitKV
	counter = 0
	kvcounter = 0
	decoder = NewDecoder(mkReader(body), 1).EmitKV()

	for mv = range decoder.Stream() {
		switch mv.Value.(type) {
		case KV:
			kvcounter++
		default:
			counter++
		}
		t.Logf("depth=%d offset=%d len=%d (%v)", mv.Depth, mv.Offset, mv.Length, mv.Value)
	}
	if err := decoder.Err(); err != nil {
		t.Fatalf("decoder error: %s", err)
	}
	if kvcounter != 15 {
		t.Fatalf("expected 15 keyvalue items, got %d", kvcounter)
	}
	if counter != 0 {
		t.Fatalf("expected 0 items, got %d", counter)
	}
}

func assertTrue(t *testing.T, a interface{}) {
	if a == false {
		t.Errorf("%+v should be true %s", a, debug.Stack())
	}
}

func assertFalse(t *testing.T, a interface{}) {
	if a == true {
		t.Errorf("%+v should be False %s", a, debug.Stack())
	}
}

func assertEqual(t *testing.T, a, b interface{}) {
	if a != b {
		t.Errorf("expected value %+v not equal to actual value %+v %s", a, b, debug.Stack())
	}
}

func assertNotNil(t *testing.T, a interface{}) {
	if a == nil {
		t.Errorf("%+v should not nil %s", a, debug.Stack())
	}
}

func assertNil(t *testing.T, a interface{}) {
	if a != nil {
		t.Errorf("%+v should be nil %s", a, debug.Stack())
	}
}
