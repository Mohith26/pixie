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
	"net"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"px.dev/pixie/src/api/proto/cloudpb"
	"px.dev/pixie/src/api/proto/vispb"
	"px.dev/pixie/src/api/proto/vizierpb"
)

const (
	httpDataID  = "11111111-1111-1111-1111-111111111111"
	plainID     = "22222222-2222-2222-2222-222222222222"
	tracepoint  = "33333333-3333-3333-3333-333333333333"
	httpDataPxL = "import px\ndef http_data(start_time: str, svc: str):\n    return px.DataFrame('http_events', start_time=start_time)\n"
)

func httpDataVis() *vispb.Vis {
	return &vispb.Vis{
		Variables: []*vispb.Vis_Variable{
			{Name: "start_time", Type: vispb.PX_STRING, DefaultValue: &types.StringValue{Value: "-5m"}},
			{Name: "svc", Type: vispb.PX_SERVICE},
			{Name: "groupby", Type: vispb.PX_STRING, DefaultValue: &types.StringValue{Value: "pod"}, ValidValues: []string{"pod", "service"}},
		},
		GlobalFuncs: []*vispb.Vis_GlobalFunc{{
			OutputName: "http",
			Func: &vispb.Widget_Func{Name: "http_data", Args: []*vispb.Widget_Func_FuncArg{
				{Name: "start_time", Input: &vispb.Widget_Func_FuncArg_Variable{Variable: "start_time"}},
				{Name: "svc", Input: &vispb.Widget_Func_FuncArg_Variable{Variable: "svc"}},
			}},
		}},
		Widgets: []*vispb.Widget{
			{Name: "table", FuncOrRef: &vispb.Widget_GlobalFuncOutputName{GlobalFuncOutputName: "http"}},
			{Name: "latency", FuncOrRef: &vispb.Widget_Func_{Func: &vispb.Widget_Func{Name: "latency", Args: []*vispb.Widget_Func_FuncArg{
				{Name: "groupby", Input: &vispb.Widget_Func_FuncArg_Variable{Variable: "groupby"}},
				{Name: "limit", Input: &vispb.Widget_Func_FuncArg_Value{Value: "100"}},
			}}}},
		},
	}
}

type fakeScriptMgr struct {
	cloudpb.UnimplementedScriptMgrServer
	apiKeys []string
}

func (f *fakeScriptMgr) record(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.apiKeys = append(f.apiKeys, md.Get("pixie-api-key")...)
}

func (f *fakeScriptMgr) GetScripts(ctx context.Context, _ *cloudpb.GetScriptsReq) (*cloudpb.GetScriptsResp, error) {
	f.record(ctx)
	return &cloudpb.GetScriptsResp{Scripts: []*cloudpb.ScriptMetadata{
		{ID: plainID, Name: "px/plain", Desc: "No vis spec."},
		{ID: httpDataID, Name: "px/http_data", Desc: "HTTP events.", HasLiveView: true},
		{ID: tracepoint, Name: "px/trace", Desc: "Adds a tracepoint."},
	}}, nil
}

func (f *fakeScriptMgr) GetLiveViewContents(ctx context.Context, req *cloudpb.GetLiveViewContentsReq) (*cloudpb.GetLiveViewContentsResp, error) {
	f.record(ctx)
	if req.LiveViewID != httpDataID {
		return nil, errors.New("unexpected live view")
	}
	return &cloudpb.GetLiveViewContentsResp{PxlContents: httpDataPxL, Vis: httpDataVis()}, nil
}

func (f *fakeScriptMgr) GetScriptContents(ctx context.Context, req *cloudpb.GetScriptContentsReq) (*cloudpb.GetScriptContentsResp, error) {
	f.record(ctx)
	switch req.ScriptID {
	case plainID:
		return &cloudpb.GetScriptContentsResp{Contents: "import px\npx.display(px.DataFrame('conn_stats'))\n"}, nil
	case tracepoint:
		return &cloudpb.GetScriptContentsResp{Contents: "import px\nimport pxtrace\n"}, nil
	}
	return nil, errors.New("unexpected script")
}

type fakeVizier struct {
	vizierpb.UnimplementedVizierServiceServer
	reqs chan *vizierpb.ExecuteScriptRequest
}

func (f *fakeVizier) ExecuteScript(req *vizierpb.ExecuteScriptRequest, _ vizierpb.VizierService_ExecuteScriptServer) error {
	f.reqs <- req
	return nil
}

func newTestClients(t *testing.T) (*Client, *VizierClient, *fakeScriptMgr, *fakeVizier) {
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	sm := &fakeScriptMgr{}
	vz := &fakeVizier{reqs: make(chan *vizierpb.ExecuteScriptRequest, 1)}
	cloudpb.RegisterScriptMgrServer(s, sm)
	vizierpb.RegisterVizierServiceServer(s, vz)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.Dial("bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	c := &Client{apiKey: "test-key", grpcConn: conn, scriptMgr: cloudpb.NewScriptMgrClient(conn)}
	v := &VizierClient{cloud: c, vizierID: "vz-1", vzClient: vizierpb.NewVizierServiceClient(conn)}
	return c, v, sm, vz
}

func receive(t *testing.T, vz *fakeVizier) *vizierpb.ExecuteScriptRequest {
	select {
	case req := <-vz.reqs:
		return req
	case <-time.After(5 * time.Second):
		t.Fatal("vizier did not receive an ExecuteScript request")
		return nil
	}
}

func argValues(f *vizierpb.ExecuteScriptRequest_FuncToExecute) map[string]string {
	out := map[string]string{}
	for _, a := range f.ArgValues {
		out[a.Name] = a.Value
	}
	return out
}

func TestListScriptsIsSortedAndAuthenticated(t *testing.T) {
	c, _, sm, _ := newTestClients(t)
	scripts, err := c.ListScripts(context.Background())
	require.NoError(t, err)
	require.Len(t, scripts, 3)
	assert.Equal(t, []string{"px/http_data", "px/plain", "px/trace"},
		[]string{scripts[0].Name, scripts[1].Name, scripts[2].Name})
	assert.Equal(t, &ScriptInfo{ID: httpDataID, Name: "px/http_data", Description: "HTTP events.", HasLiveView: true}, scripts[0])
	assert.Equal(t, []string{"test-key"}, sm.apiKeys)
}

func TestGetScriptFetchesLiveViewWithVis(t *testing.T) {
	c, _, _, _ := newTestClients(t)
	s, err := c.GetScript(context.Background(), "px/http_data")
	require.NoError(t, err)
	assert.Equal(t, httpDataPxL, s.PxL)
	require.NotNil(t, s.Vis)
	vars := s.Variables()
	require.Len(t, vars, 3)
	assert.Equal(t, ScriptVariable{Name: "start_time", Type: "PX_STRING", DefaultValue: "-5m"}, vars[0])
	assert.Equal(t, ScriptVariable{Name: "svc", Type: "PX_SERVICE", Required: true}, vars[1])
	assert.Equal(t, []string{"pod", "service"}, vars[2].ValidValues)
}

func TestGetScriptFetchesPlainScript(t *testing.T) {
	c, _, _, _ := newTestClients(t)
	s, err := c.GetScript(context.Background(), "px/plain")
	require.NoError(t, err)
	assert.Nil(t, s.Vis)
	assert.Nil(t, s.Variables())
	assert.Contains(t, s.PxL, "conn_stats")
}

func TestGetScriptNotFound(t *testing.T) {
	c, _, _, _ := newTestClients(t)
	_, err := c.GetScript(context.Background(), "px/does_not_exist")
	assert.True(t, errors.Is(err, ErrScriptNotFound))
}

func TestExecuteScriptByNameBindsArgsLikeTheCLI(t *testing.T) {
	_, v, _, vz := newTestClients(t)
	res, err := v.ExecuteScriptByName(context.Background(), "px/http_data", map[string]string{"svc": "default/cart"}, nil)
	require.NoError(t, err)
	defer res.Close()
	req := receive(t, vz)

	assert.Equal(t, "vz-1", req.ClusterID)
	assert.Equal(t, httpDataPxL, req.QueryStr)
	assert.Equal(t, "px/http_data", req.QueryName)
	assert.False(t, req.Mutation)
	// The global func runs once under its output name; the widget that references it adds no
	// function, and the widget with its own func runs under the widget's name.
	require.Len(t, req.ExecFuncs, 2)
	assert.Equal(t, "http_data", req.ExecFuncs[0].FuncName)
	assert.Equal(t, "http", req.ExecFuncs[0].OutputTablePrefix)
	assert.Equal(t, map[string]string{"start_time": "-5m", "svc": "default/cart"}, argValues(req.ExecFuncs[0]))
	assert.Equal(t, "latency", req.ExecFuncs[1].FuncName)
	assert.Equal(t, "latency", req.ExecFuncs[1].OutputTablePrefix)
	assert.Equal(t, map[string]string{"groupby": "pod", "limit": "100"}, argValues(req.ExecFuncs[1]))
	// Argument order follows the vis spec.
	assert.Equal(t, "start_time", req.ExecFuncs[0].ArgValues[0].Name)
}

func TestExecuteNamedScriptReusesFetchedScript(t *testing.T) {
	c, v, sm, vz := newTestClients(t)
	s, err := c.GetScript(context.Background(), "px/http_data")
	require.NoError(t, err)
	calls := len(sm.apiKeys)
	for _, svc := range []string{"a/one", "b/two"} {
		res, err := v.ExecuteNamedScript(context.Background(), s, map[string]string{"svc": svc, "groupby": "service", "start_time": "-1h"}, nil)
		require.NoError(t, err)
		req := receive(t, vz)
		assert.Equal(t, map[string]string{"start_time": "-1h", "svc": svc}, argValues(req.ExecFuncs[0]))
		assert.Equal(t, "service", argValues(req.ExecFuncs[1])["groupby"])
		res.Close()
	}
	assert.Equal(t, calls, len(sm.apiKeys), "executing a fetched script must not call the cloud again")
}

func TestExecuteScriptByNameRejectsBadArgs(t *testing.T) {
	_, v, _, _ := newTestClients(t)
	ctx := context.Background()
	cases := map[string]map[string]string{
		"missing required argument \"svc\"":                                {},
		"unknown arguments [nope, typo]":                                   {"svc": "a/b", "typo": "x", "nope": "y"},
		"argument \"groupby\" must be one of [pod, service], got \"node\"": {"svc": "a/b", "groupby": "node"},
	}
	for want, args := range cases {
		_, err := v.ExecuteScriptByName(ctx, "px/http_data", args, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), want)
		assert.Contains(t, err.Error(), "px/http_data")
	}
}

func TestExecuteScriptByNamePlainAndMutationScripts(t *testing.T) {
	_, v, _, vz := newTestClients(t)
	ctx := context.Background()

	res, err := v.ExecuteScriptByName(ctx, "px/plain", nil, nil)
	require.NoError(t, err)
	req := receive(t, vz)
	assert.Empty(t, req.ExecFuncs)
	assert.False(t, req.Mutation)
	res.Close()

	_, err = v.ExecuteScriptByName(ctx, "px/plain", map[string]string{"x": "1"}, nil)
	assert.ErrorContains(t, err, "unknown arguments [x]")

	res, err = v.ExecuteScriptByName(ctx, "px/trace", nil, nil)
	require.NoError(t, err)
	assert.True(t, receive(t, vz).Mutation)
	res.Close()
}

func TestExecuteScriptStillSendsRawPxL(t *testing.T) {
	_, v, _, vz := newTestClients(t)
	res, err := v.ExecuteScript(context.Background(), "import px\n", nil)
	require.NoError(t, err)
	req := receive(t, vz)
	assert.Equal(t, "import px\n", req.QueryStr)
	assert.Equal(t, "vz-1", req.ClusterID)
	assert.Empty(t, req.ExecFuncs)
	assert.Empty(t, req.QueryName)
	res.Close()
}
