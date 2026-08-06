package agnt5

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
)

const (
	durableActivationV1Capability       = "durable_activation_v1"
	durableSuspensionV1Capability       = "durable_suspension_v1"
	activationArtifactSHA256Metadata    = "activation_artifact_sha256"
	activationDefinitionVersionMetadata = "activation_definition_version"
	activationDefinitionConfigMetadata  = "activation_definition_config"
)

// RecoveryPolicy controls how interrupted durable work is settled.
type RecoveryPolicy string

const (
	RecoveryPolicyIdempotentRetry RecoveryPolicy = "idempotent_retry"
	RecoveryPolicyDurableSteps    RecoveryPolicy = "durable_steps"
	RecoveryPolicyUnknownOutcome  RecoveryPolicy = "unknown_outcome"
	RecoveryPolicyCompensate      RecoveryPolicy = "compensate"
	RecoveryPolicyFail            RecoveryPolicy = "fail"
)

// ActivationExecution is exposed while one durable unit is running.
type ActivationExecution struct {
	ActivationID   string
	Attempt        uint32
	IdempotencyKey string
}

func recoveryPolicyProto(policy RecoveryPolicy) (pb.ActivationRecoveryPolicy, error) {
	switch policy {
	case RecoveryPolicyIdempotentRetry:
		return pb.ActivationRecoveryPolicy_ACTIVATION_RECOVERY_POLICY_IDEMPOTENT_RETRY, nil
	case RecoveryPolicyDurableSteps:
		return pb.ActivationRecoveryPolicy_ACTIVATION_RECOVERY_POLICY_DURABLE_STEPS, nil
	case RecoveryPolicyUnknownOutcome:
		return pb.ActivationRecoveryPolicy_ACTIVATION_RECOVERY_POLICY_UNKNOWN_OUTCOME, nil
	case RecoveryPolicyCompensate:
		return pb.ActivationRecoveryPolicy_ACTIVATION_RECOVERY_POLICY_COMPENSATE, nil
	case RecoveryPolicyFail:
		return pb.ActivationRecoveryPolicy_ACTIVATION_RECOVERY_POLICY_FAIL, nil
	default:
		return pb.ActivationRecoveryPolicy_ACTIVATION_RECOVERY_POLICY_UNSPECIFIED, fmt.Errorf("agnt5: unsupported recovery policy %q", policy)
	}
}

type activationPlan struct {
	stableKey        string
	inputDigest      []byte
	definitionDigest []byte
}

func activationPlanForStep(ctx *Context, stableKey string, input any) (activationPlan, bool, error) {
	if ctx.Metadata(durableActivationV1Capability) != "true" {
		return activationPlan{}, false, nil
	}
	if ctx.activationWriter == nil {
		return activationPlan{}, true, newActivationError(
			ActivationErrorDurabilityUnavailable,
			"runtime negotiated durable_activation_v1 but no activation writer is configured",
			"",
			0,
			nil,
		)
	}
	canonicalInput, err := canonicalActivationValue(input)
	if err != nil {
		return activationPlan{}, true, newActivationError(ActivationErrorInvalidArgument, err.Error(), "", 0, err)
	}
	definitionDigest, err := activationDefinitionDigestFromContext(ctx)
	if err != nil {
		return activationPlan{}, true, err
	}
	inputDigest := sha256.Sum256(canonicalInput)
	return activationPlan{
		stableKey:        stableKey,
		inputDigest:      inputDigest[:],
		definitionDigest: definitionDigest,
	}, true, nil
}

func (w *Worker) protocolRegistrationCapabilities() (supported, required []string) {
	if w.durableActivationMode == DurableActivationDisabled {
		return nil, nil
	}
	supported = []string{durableActivationV1Capability, durableSuspensionV1Capability}
	if w.durableActivationMode == DurableActivationRequired {
		required = []string{durableActivationV1Capability}
	}
	return supported, required
}

func (w *Worker) applyProtocolNegotiation(runtimeSupported, runtimeRequired []string) error {
	workerSupported, _ := w.protocolRegistrationCapabilities()
	for _, required := range runtimeRequired {
		if !stringSliceContains(workerSupported, required) {
			return newActivationError(
				ActivationErrorDurabilityUnavailable,
				"runtime requires unsupported worker protocol capability: "+required,
				"",
				0,
				nil,
			)
		}
	}
	enabled := stringSliceContains(runtimeSupported, durableActivationV1Capability) &&
		stringSliceContains(workerSupported, durableActivationV1Capability)
	suspensionEnabled := stringSliceContains(runtimeSupported, durableSuspensionV1Capability) &&
		stringSliceContains(workerSupported, durableSuspensionV1Capability)
	definitionReason := ""
	if enabled {
		if _, err := decodeSHA256(w.metadata[activationArtifactSHA256Metadata]); err != nil {
			enabled = false
			definitionReason = "activation artifact identity is unavailable; configure activation_artifact_sha256"
		}
	}
	if w.durableActivationMode == DurableActivationRequired && !enabled {
		message := "worker requires durable_activation_v1 but the runtime did not negotiate it"
		if definitionReason != "" {
			message = "worker requires durable_activation_v1 but " + definitionReason
		}
		return newActivationError(
			ActivationErrorDurabilityUnavailable,
			message,
			"",
			0,
			nil,
		)
	}
	reason := ""
	if w.durableActivationMode == DurableActivationPreferred && !enabled {
		if definitionReason != "" {
			reason = definitionReason + "; legacy checkpoints remain enabled"
		} else {
			reason = "runtime did not advertise durable_activation_v1; legacy checkpoints remain enabled"
		}
	}
	w.protocolMu.Lock()
	previousReason := w.durableActivationWhy
	w.durableActivationOn = enabled
	w.durableSuspensionOn = enabled && suspensionEnabled
	w.durableActivationWhy = reason
	w.protocolMu.Unlock()
	if reason != "" && reason != previousReason {
		fmt.Fprintf(os.Stderr, "[WARN] agnt5 durable activation degraded: %s\n", reason)
	}
	return nil
}

func stringSliceContains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (w *Worker) withActivationMetadata(inv Invocation, component Component) Invocation {
	inv.Metadata = w.invocationMetadata(inv)
	if status := w.DurableActivationStatus(); status.Enabled {
		inv.Metadata[durableActivationV1Capability] = "true"
		inv.Metadata[activationDefinitionVersionMetadata] = w.serviceVersion
		if componentConfig, err := canonicalActivationValue(component.Config); err == nil {
			inv.Metadata[activationDefinitionConfigMetadata] = string(componentConfig)
		}
	}
	w.protocolMu.RLock()
	durableSuspensionOn := w.durableSuspensionOn
	w.protocolMu.RUnlock()
	if durableSuspensionOn {
		inv.Metadata[durableSuspensionV1Capability] = "true"
	}
	return inv
}

func activationDefinitionDigestFromContext(ctx *Context) ([]byte, error) {
	artifact, err := decodeSHA256(ctx.Metadata(activationArtifactSHA256Metadata))
	if err != nil {
		return nil, newActivationError(ActivationErrorDurabilityUnavailable, "activation artifact SHA-256 is unavailable: "+err.Error(), "", 0, err)
	}
	version := ctx.Metadata(activationDefinitionVersionMetadata)
	if version == "" {
		return nil, newActivationError(ActivationErrorDurabilityUnavailable, "activation definition version is unavailable", "", 0, nil)
	}
	config := ctx.Metadata(activationDefinitionConfigMetadata)
	if config == "" {
		config = `["object",[]]`
	}
	if !json.Valid([]byte(config)) {
		return nil, newActivationError(ActivationErrorInvalidArgument, "activation definition config is not valid canonical JSON", "", 0, nil)
	}
	return activationDefinitionDigest(artifact, ctx.ComponentName(), version, []byte(config)), nil
}

func activationDefinitionDigest(artifact []byte, component, version string, canonicalConfig []byte) []byte {
	h := sha256.New()
	h.Write([]byte("agnt5.activation.definition.v1\x00"))
	for _, value := range [][]byte{artifact, []byte(component), []byte(version), []byte(durableActivationV1Capability), canonicalConfig} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		h.Write(length[:])
		h.Write(value)
	}
	return h.Sum(nil)
}

func activationID(projectID, runID, parentID string, kind pb.ActivationKind, stableKey string) string {
	h := sha256.New()
	h.Write([]byte("agnt5.activation.identity.v1\x00"))
	for _, value := range []string{projectID, runID, parentID} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len([]byte(value))))
		h.Write(length[:])
		h.Write([]byte(value))
	}
	var kindBytes [4]byte
	binary.BigEndian.PutUint32(kindBytes[:], uint32(kind))
	h.Write(kindBytes[:])
	var stableKeyLength [8]byte
	binary.BigEndian.PutUint64(stableKeyLength[:], uint64(len([]byte(stableKey))))
	h.Write(stableKeyLength[:])
	h.Write([]byte(stableKey))
	return "actv1_" + base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

func decodeSHA256(value string) ([]byte, error) {
	if value == "" {
		return nil, fmt.Errorf("value is empty")
	}
	for _, decoder := range []func(string) ([]byte, error){
		hex.DecodeString,
		base64.StdEncoding.DecodeString,
		base64.RawStdEncoding.DecodeString,
		base64.RawURLEncoding.DecodeString,
	} {
		decoded, err := decoder(value)
		if err == nil && len(decoded) == sha256.Size {
			return decoded, nil
		}
	}
	return nil, fmt.Errorf("value must encode exactly 32 bytes")
}

func inlineActivationPayload(data []byte) *pb.ActivationPayload {
	return &pb.ActivationPayload{Value: &pb.ActivationPayload_InlineData{InlineData: cloneBytes(data)}}
}

func inlineActivationBytes(payload *pb.ActivationPayload) ([]byte, error) {
	if payload == nil {
		return nil, newActivationError(ActivationErrorUnknownOutcome, "activation receipt has no payload", "", 0, nil)
	}
	inline, ok := payload.GetValue().(*pb.ActivationPayload_InlineData)
	if !ok {
		return nil, newActivationError(ActivationErrorReferenceRequired, "referenced activation outputs are not supported by this Go SDK version", "", 0, nil)
	}
	return cloneBytes(inline.InlineData), nil
}

func canonicalActivationValue(value any) ([]byte, error) {
	var out bytes.Buffer
	if err := appendCanonicalActivationValue(&out, reflect.ValueOf(value)); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

var (
	activationTimeType      = reflect.TypeOf(time.Time{})
	activationDurationType  = reflect.TypeOf(time.Duration(0))
	activationByteSliceType = reflect.TypeOf([]byte(nil))
)

func appendCanonicalActivationValue(out *bytes.Buffer, value reflect.Value) error {
	if !value.IsValid() {
		out.WriteString(`["null"]`)
		return nil
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			out.WriteString(`["null"]`)
			return nil
		}
		value = value.Elem()
	}
	if value.Type() == activationTimeType {
		out.WriteString(`["timestamp_ns","`)
		out.WriteString(strconv.FormatInt(value.Interface().(time.Time).UnixNano(), 10))
		out.WriteString(`"]`)
		return nil
	}
	if value.Type() == activationDurationType {
		out.WriteString(`["duration_ns","`)
		out.WriteString(strconv.FormatInt(int64(value.Interface().(time.Duration)), 10))
		out.WriteString(`"]`)
		return nil
	}
	if value.Type() == activationByteSliceType {
		if value.IsNil() {
			out.WriteString(`["null"]`)
			return nil
		}
		out.WriteString(`["bytes","`)
		out.WriteString(base64.RawURLEncoding.EncodeToString(value.Bytes()))
		out.WriteString(`"]`)
		return nil
	}

	switch value.Kind() {
	case reflect.Bool:
		out.WriteString(`["bool",`)
		out.WriteString(strconv.FormatBool(value.Bool()))
		out.WriteByte(']')
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		out.WriteString(`["i64","`)
		out.WriteString(strconv.FormatInt(value.Int(), 10))
		out.WriteString(`"]`)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		out.WriteString(`["u64","`)
		out.WriteString(strconv.FormatUint(value.Uint(), 10))
		out.WriteString(`"]`)
	case reflect.Float32, reflect.Float64:
		floatValue := value.Convert(reflect.TypeOf(float64(0))).Float()
		if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
			return fmt.Errorf("agnt5: canonical activation values reject NaN and infinity")
		}
		bits := math.Float64bits(floatValue)
		if floatValue == 0 {
			bits = 0
		}
		out.WriteString(`["f64","`)
		fmt.Fprintf(out, "%016x", bits)
		out.WriteString(`"]`)
	case reflect.String:
		out.WriteString(`["string",`)
		if err := appendCanonicalJSONString(out, value.String()); err != nil {
			return err
		}
		out.WriteByte(']')
	case reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice && value.IsNil() {
			out.WriteString(`["null"]`)
			return nil
		}
		out.WriteString(`["array",[`)
		for index := 0; index < value.Len(); index++ {
			if index > 0 {
				out.WriteByte(',')
			}
			if err := appendCanonicalActivationValue(out, value.Index(index)); err != nil {
				return err
			}
		}
		out.WriteString(`]]`)
	case reflect.Map:
		if value.IsNil() {
			out.WriteString(`["null"]`)
			return nil
		}
		if value.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("agnt5: canonical activation objects require string keys")
		}
		keys := value.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
		out.WriteString(`["object",[`)
		for index, key := range keys {
			if index > 0 {
				out.WriteByte(',')
			}
			out.WriteByte('[')
			if err := appendCanonicalJSONString(out, key.String()); err != nil {
				return err
			}
			out.WriteByte(',')
			if err := appendCanonicalActivationValue(out, value.MapIndex(key)); err != nil {
				return err
			}
			out.WriteByte(']')
		}
		out.WriteString(`]]`)
	case reflect.Struct:
		return appendCanonicalStruct(out, value)
	default:
		return fmt.Errorf("agnt5: unsupported canonical activation value type %s", value.Type())
	}
	return nil
}

type canonicalStructField struct {
	name  string
	value reflect.Value
}

func appendCanonicalStruct(out *bytes.Buffer, value reflect.Value) error {
	fields := make([]canonicalStructField, 0, value.NumField())
	typeInfo := value.Type()
	for index := 0; index < value.NumField(); index++ {
		fieldInfo := typeInfo.Field(index)
		if fieldInfo.PkgPath != "" {
			continue
		}
		tag := fieldInfo.Tag.Get("json")
		parts := strings.Split(tag, ",")
		if parts[0] == "-" {
			continue
		}
		name := parts[0]
		if name == "" {
			name = fieldInfo.Name
		}
		fieldValue := value.Field(index)
		if len(parts) > 1 && slicesContain(parts[1:], "omitempty") && fieldValue.IsZero() {
			continue
		}
		fields = append(fields, canonicalStructField{name: name, value: fieldValue})
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].name < fields[j].name })
	out.WriteString(`["object",[`)
	for index, field := range fields {
		if index > 0 {
			out.WriteByte(',')
		}
		out.WriteByte('[')
		if err := appendCanonicalJSONString(out, field.name); err != nil {
			return err
		}
		out.WriteByte(',')
		if err := appendCanonicalActivationValue(out, field.value); err != nil {
			return err
		}
		out.WriteByte(']')
	}
	out.WriteString(`]]`)
	return nil
}

func slicesContain(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func appendCanonicalJSONString(out *bytes.Buffer, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("agnt5: canonical activation strings must contain valid UTF-8")
	}
	out.WriteByte('"')
	for _, char := range value {
		switch char {
		case '"', '\\':
			out.WriteByte('\\')
			out.WriteRune(char)
		case '\b':
			out.WriteString(`\b`)
		case '\t':
			out.WriteString(`\t`)
		case '\n':
			out.WriteString(`\n`)
		case '\f':
			out.WriteString(`\f`)
		case '\r':
			out.WriteString(`\r`)
		default:
			if char >= 0 && char <= 0x1f {
				fmt.Fprintf(out, `\u%04x`, char)
			} else {
				out.WriteRune(char)
			}
		}
	}
	out.WriteByte('"')
	return nil
}
