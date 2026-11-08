/*
 * Copyright 2018- The Pixie Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package pxapi

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"px.dev/pixie/src/api/proto/cloudpb"
	"px.dev/pixie/src/api/proto/vispb"
	"px.dev/pixie/src/api/proto/vizierpb"
)

// ErrScriptNotFound is returned when no script with the requested name is available.
var ErrScriptNotFound = errors.New("script not found")

// ScriptInfo describes a script that is available in Pixie Cloud, such as `px/http_data`.
type ScriptInfo struct {
	// ID of the script (uuid as a string).
	ID string
	// Name of the script, for example `px/perf_flamegraph`.
	Name string
	// Description of what the script does.
	Description string
	// HasLiveView is true when the script ships a vis spec and can be shown as a live view.
	HasLiveView bool
}

// ScriptVariable is an argument that a script accepts.
type ScriptVariable struct {
	// Name of the argument.
	Name string
	// Type of the argument, for example `PX_STRING` or `PX_SERVICE`.
	Type string
	// Description of the argument.
	Description string
	// DefaultValue is used when the caller does not pass the argument.
	DefaultValue string
	// Required is true when the argument has no default value and must be passed.
	Required bool
	// ValidValues optionally restricts the values the argument accepts.
	ValidValues []string
}

// Script is a script fetched from Pixie Cloud, ready to be executed on a Vizier.
type Script struct {
	ScriptInfo
	// PxL is the source of the script.
	PxL string
	// Vis is the vis spec of the script; nil for scripts without a live view.
	Vis *vispb.Vis
}

// Variables lists the arguments the script accepts, in the order the vis spec declares them.
func (s *Script) Variables() []ScriptVariable {
	if s.Vis == nil {
		return nil
	}
	vars := make([]ScriptVariable, 0, len(s.Vis.Variables))
	for _, v := range s.Vis.Variables {
		sv := ScriptVariable{
			Name:        v.Name,
			Type:        v.Type.String(),
			Description: v.Description,
			Required:    v.DefaultValue == nil,
			ValidValues: v.ValidValues,
		}
		if v.DefaultValue != nil {
			sv.DefaultValue = v.DefaultValue.Value
		}
		vars = append(vars, sv)
	}
	return vars
}

// ListScripts returns the scripts available in Pixie Cloud, sorted by name.
func (c *Client) ListScripts(ctx context.Context) ([]*ScriptInfo, error) {
	resp, err := c.scriptMgr.GetScripts(c.cloudCtxWithMD(ctx), &cloudpb.GetScriptsReq{})
	if err != nil {
		return nil, err
	}
	scripts := make([]*ScriptInfo, 0, len(resp.Scripts))
	for _, s := range resp.Scripts {
		scripts = append(scripts, scriptInfoFromProto(s))
	}
	sort.Slice(scripts, func(i, j int) bool { return scripts[i].Name < scripts[j].Name })
	return scripts, nil
}

// GetScript fetches a script and, if it has one, its vis spec from Pixie Cloud by name,
// for example `px/http_data`. It returns an error wrapping ErrScriptNotFound when no script
// has that name.
func (c *Client) GetScript(ctx context.Context, name string) (*Script, error) {
	scripts, err := c.ListScripts(ctx)
	if err != nil {
		return nil, err
	}
	var info *ScriptInfo
	for _, s := range scripts {
		if s.Name == name {
			info = s
			break
		}
	}
	if info == nil {
		return nil, fmt.Errorf("%w: %q", ErrScriptNotFound, name)
	}

	ctx = c.cloudCtxWithMD(ctx)
	if info.HasLiveView {
		resp, err := c.scriptMgr.GetLiveViewContents(ctx, &cloudpb.GetLiveViewContentsReq{LiveViewID: info.ID})
		if err != nil {
			return nil, err
		}
		return &Script{ScriptInfo: *info, PxL: resp.PxlContents, Vis: resp.Vis}, nil
	}
	resp, err := c.scriptMgr.GetScriptContents(ctx, &cloudpb.GetScriptContentsReq{ScriptID: info.ID})
	if err != nil {
		return nil, err
	}
	return &Script{ScriptInfo: *info, PxL: resp.Contents}, nil
}

// ExecuteScriptByName fetches the named script from Pixie Cloud and runs it on this Vizier,
// the way `px run <name>` does. args sets the script's variables (see Script.Variables);
// variables that are not passed use their defaults.
func (v *VizierClient) ExecuteScriptByName(ctx context.Context, name string, args map[string]string, mux TableMuxer) (*ScriptResults, error) {
	script, err := v.cloud.GetScript(ctx, name)
	if err != nil {
		return nil, err
	}
	return v.ExecuteNamedScript(ctx, script, args, mux)
}

// ExecuteNamedScript runs a script returned by Client.GetScript on this Vizier, binding args to
// the variables of its vis spec. It can be called many times with different args without
// fetching the script again.
func (v *VizierClient) ExecuteNamedScript(ctx context.Context, script *Script, args map[string]string, mux TableMuxer) (*ScriptResults, error) {
	if script == nil {
		return nil, errors.New("script is nil")
	}
	execFuncs, err := funcsToExecute(script.Vis, args)
	if err != nil {
		return nil, fmt.Errorf("script %q: %w", script.Name, err)
	}
	return v.execute(ctx, &vizierpb.ExecuteScriptRequest{
		QueryStr:  script.PxL,
		ExecFuncs: execFuncs,
		QueryName: script.Name,
		Mutation:  containsMutation(script.PxL),
	}, mux)
}

func scriptInfoFromProto(s *cloudpb.ScriptMetadata) *ScriptInfo {
	return &ScriptInfo{ID: s.ID, Name: s.Name, Description: s.Desc, HasLiveView: s.HasLiveView}
}

// resolveArgs binds args to the variables of the vis spec, applying defaults and rejecting
// unknown names, missing required values and values outside valid_values.
func resolveArgs(vis *vispb.Vis, args map[string]string) (map[string]string, error) {
	resolved := make(map[string]string)
	known := make(map[string]bool)
	if vis != nil {
		for _, variable := range vis.Variables {
			known[variable.Name] = true
			val, ok := args[variable.Name]
			if !ok {
				if variable.DefaultValue == nil {
					return nil, fmt.Errorf("missing required argument %q", variable.Name)
				}
				val = variable.DefaultValue.Value
			} else if len(variable.ValidValues) > 0 && !contains(variable.ValidValues, val) {
				return nil, fmt.Errorf("argument %q must be one of [%s], got %q",
					variable.Name, strings.Join(variable.ValidValues, ", "), val)
			}
			resolved[variable.Name] = val
		}
	}
	var unknown []string
	for name := range args {
		if !known[name] {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("unknown arguments [%s]", strings.Join(unknown, ", "))
	}
	return resolved, nil
}

// funcsToExecute lists the global and widget functions of the vis spec with their argument
// values, as the CLI sends them for `px run`.
func funcsToExecute(vis *vispb.Vis, args map[string]string) ([]*vizierpb.ExecuteScriptRequest_FuncToExecute, error) {
	resolved, err := resolveArgs(vis, args)
	if err != nil {
		return nil, err
	}
	execFuncs := []*vizierpb.ExecuteScriptRequest_FuncToExecute{}
	if vis == nil {
		return execFuncs, nil
	}
	for _, f := range vis.GlobalFuncs {
		ef, err := funcToExecute(f.Func, resolved, f.OutputName)
		if err != nil {
			return nil, err
		}
		execFuncs = append(execFuncs, ef)
	}
	for _, w := range vis.Widgets {
		wf, ok := w.FuncOrRef.(*vispb.Widget_Func_)
		if !ok {
			// Widgets that reference a global func are covered above.
			continue
		}
		ef, err := funcToExecute(wf.Func, resolved, w.Name)
		if err != nil {
			return nil, err
		}
		execFuncs = append(execFuncs, ef)
	}
	return execFuncs, nil
}

func funcToExecute(f *vispb.Widget_Func, resolved map[string]string, name string) (*vizierpb.ExecuteScriptRequest_FuncToExecute, error) {
	ef := &vizierpb.ExecuteScriptRequest_FuncToExecute{
		FuncName:          f.Name,
		ArgValues:         make([]*vizierpb.ExecuteScriptRequest_FuncToExecute_ArgValue, len(f.Args)),
		OutputTablePrefix: "widget",
	}
	if name != "" {
		ef.OutputTablePrefix = name
	}
	for i, arg := range f.Args {
		var value string
		switch x := arg.Input.(type) {
		case *vispb.Widget_Func_FuncArg_Value:
			value = x.Value
		case *vispb.Widget_Func_FuncArg_Variable:
			val, ok := resolved[x.Variable]
			if !ok {
				return nil, fmt.Errorf("function %q binds argument %q to undeclared variable %q", f.Name, arg.Name, x.Variable)
			}
			value = val
		default:
			return nil, fmt.Errorf("function %q argument %q has no value", f.Name, arg.Name)
		}
		ef.ArgValues[i] = &vizierpb.ExecuteScriptRequest_FuncToExecute_ArgValue{Name: arg.Name, Value: value}
	}
	return ef, nil
}

// mutationRe matches the same pxtrace imports the CLI checks for before sending a mutation.
var mutationRe = regexp.MustCompile(`(?m:^(from pxtrace|import pxtrace)$)`)

// containsMutation reports whether the script imports pxtrace, which the CLI sends as a mutation.
func containsMutation(pxl string) bool {
	return mutationRe.MatchString(pxl)
}

func contains(values []string, v string) bool {
	for _, x := range values {
		if x == v {
			return true
		}
	}
	return false
}
