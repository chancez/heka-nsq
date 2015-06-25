package nsq

import (
	"fmt"
	"github.com/mozilla-services/heka/message"
	"regexp"
	"strconv"
)

var fieldRegex = regexp.MustCompile("^Fields\\[([^\\]]*)\\](?:\\[(\\d+)\\])?(?:\\[(\\d+)\\])?$")

type messageVariable struct {
	header bool
	name   string
	fi     int
	ai     int
}

func verifyMessageVariable(key string) *messageVariable {
	switch key {
	case "Type", "Logger", "Hostname", "Payload":
		return &messageVariable{header: true, name: key}
	default:
		matches := fieldRegex.FindStringSubmatch(key)
		if len(matches) == 4 {
			mvar := &messageVariable{header: false, name: matches[1]}
			if len(matches[2]) > 0 {
				if parsedInt, err := strconv.ParseInt(matches[2], 10, 32); err == nil {
					mvar.fi = int(parsedInt)
				} else {
					return nil
				}
			}
			if len(matches[3]) > 0 {
				if parsedInt, err := strconv.ParseInt(matches[3], 10, 32); err == nil {
					mvar.ai = int(parsedInt)
				} else {
					return nil
				}
			}
			return mvar
		}
		return nil
	}
}

func getFieldAsString(msg *message.Message, mvar *messageVariable) string {
	var field *message.Field
	if mvar.fi != 0 {
		fields := msg.FindAllFields(mvar.name)
		if mvar.fi >= len(fields) {
			return ""
		}
		field = fields[mvar.fi]
	} else {
		if field = msg.FindFirstField(mvar.name); field == nil {
			return ""
		}
	}
	switch field.GetValueType() {
	case message.Field_STRING:
		if mvar.ai >= len(field.ValueString) {
			return ""
		}
		return field.ValueString[mvar.ai]
	case message.Field_BYTES:
		if mvar.ai >= len(field.ValueBytes) {
			return ""
		}
		return string(field.ValueBytes[mvar.ai])
	case message.Field_INTEGER:
		if mvar.ai >= len(field.ValueInteger) {
			return ""
		}
		return fmt.Sprintf("%d", field.ValueInteger[mvar.ai])
	case message.Field_DOUBLE:
		if mvar.ai >= len(field.ValueDouble) {
			return ""
		}
		return fmt.Sprintf("%g", field.ValueDouble[mvar.ai])
	case message.Field_BOOL:
		if mvar.ai >= len(field.ValueBool) {
			return ""
		}
		return fmt.Sprintf("%t", field.ValueBool[mvar.ai])
	}
	return ""
}

func getMessageVariable(msg *message.Message, mvar *messageVariable) string {
	if mvar.header {
		switch mvar.name {
		case "Type":
			return msg.GetType()
		case "Logger":
			return msg.GetLogger()
		case "Hostname":
			return msg.GetHostname()
		case "Payload":
			return msg.GetPayload()
		default:
			return ""
		}
	} else {
		return getFieldAsString(msg, mvar)
	}
}
