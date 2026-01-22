//  Copyright (c) 2020 Cisco and/or its affiliates.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at:
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package binapigen

import (
	"fmt"
	"path"

	"github.com/sirupsen/logrus"
)

func init() {
	RegisterPlugin("rpc", GenerateRPC)
}

// library dependencies
const (
	contextPkg = GoImportPath("context")
	ioPkg      = GoImportPath("io")
)

// generated names
const (
	serviceApiName    = "RPCService"    // name for the RPC service interface
	serviceImplName   = "serviceClient" // name for the RPC service implementation
	serviceClientName = "ServiceClient" // name for the RPC service client

	// TODO: register service descriptor
	//serviceDescType = "ServiceDesc"             // name for service descriptor type
	//serviceDescName = "_ServiceRPC_serviceDesc" // name for service descriptor var
)

func GenerateRPC(gen *Generator, file *File) *GenFile {
	// skip if no service is defined for file
	if file.Service == nil {
		return nil
	}

	logf("----------------------------")
	logf(" Generate RPC - %s", file.Desc.Name)
	logf("----------------------------")

	filename := path.Join(file.FilenamePrefix, file.Desc.Name+"_rpc"+generatedFilenameSuffix)
	g := gen.NewGenFile(filename, file)

	// file header
	genCodeGeneratedComment(g)
	g.P()
	g.P("package ", file.PackageName)
	g.P()

	// service RPCs
	if len(file.Service.RPCs) > 0 {
		genService(g, file.Service)
	}

	return g
}

func genService(g *GenFile, svc *Service) {
	// generate comment
	g.P("// ", serviceApiName, " defines RPC service ", g.file.Desc.Name, ".")

	// generate service interface
	g.P("type ", serviceApiName, " interface {")
	for _, rpc := range svc.RPCs {
		g.P(rpcMethodSignature(g, rpc))
		if len(rpc.VPP.Events) > 0 {
			g.P(rpcMethodSignatureWatch(g, rpc))
		}
	}
	g.P("}")
	g.P()

	// generate client implementation
	g.P("type ", serviceImplName, " struct {")
	g.P("conn ", govppApiPkg.Ident("Connection"))
	g.P("}")
	g.P()

	// generate client constructor
	g.P("func New", serviceClientName, "(conn ", govppApiPkg.Ident("Connection"), ") ", serviceApiName, " {")
	g.P("return &", serviceImplName, "{conn}")
	g.P("}")
	g.P()

	msgControlPingReply, ok := g.gen.messagesByName["control_ping_reply"]
	if !ok {
		logrus.Fatalf("no message for %v", "control_ping_reply")
	}
	msgControlPing, ok := g.gen.messagesByName["control_ping"]
	if !ok {
		logrus.Fatalf("no message for %v", "control_ping")
	}

	for _, rpc := range svc.RPCs {
		logf(" gen RPC: %v (%s)", rpc.GoName, rpc.VPP.Request)

		if len(rpc.VPP.Events) > 0 {
			streamImpl := fmt.Sprintf("%s_%sClient", serviceImplName, rpc.GoName)
			// streamApi := fmt.Sprintf("%s_%sClient", serviceApiName, rpc.GoName)

			g.P("func (c *", serviceImplName, ") ", rpcMethodSignatureWatch(g, rpc), " {")

			for i, evt := range rpc.MsgEvents {
				g.P(fmt.Sprintf("	w%d", i), ", err := c.conn.WatchEvent(ctx, (*", evt.GoIdent, ")(nil))")
				g.P("	if err != nil {")
				for j := 0; j < i; j++ {
					g.P("	w", j, ".Close()")
				}
				g.P("		return nil, err")
				g.P("	}")
			}
			g.P("	return &", streamImpl, "{")
			g.P("		watch: [", len(rpc.MsgEvents), "]api.Watcher{")
			for i := range rpc.MsgEvents {
				g.P(fmt.Sprintf("w%d,", i))
			}
			g.P("	}}, nil")
			g.P("}")

			g.P("type ", streamImpl, " struct {")
			g.P("	watch [", len(rpc.MsgEvents), "]api.Watcher")
			g.P("}")
			g.P()

			g.P("func (w *", streamImpl, ") Close() {")
			g.P("	for _, wch := range w.watch {")
			g.P("		wch.Close()")
			g.P("	}")
			g.P("}")
			g.P()
			for i, evt := range rpc.MsgEvents {
				g.P("func (w *", streamImpl, ") Get", evt.GoIdent, "() <-chan ", evt.GoIdent, " {")
				g.P("	evts := make(chan ", evt.GoIdent, ", 100)")
				g.P("	go func() {")
				g.P("		evt := w.watch[", i, "].Events()")
				g.P("		for e := range evt {")
				g.P("			evts <- *e.(*", evt.GoIdent, ")")
				g.P("		}")
				g.P("	}()")
				g.P("	return evts")
				g.P("}")
				g.P()
			}
		}

		g.P("func (c *", serviceImplName, ") ", rpcMethodSignature(g, rpc), " {")
		if rpc.VPP.Stream {
			streamImpl := fmt.Sprintf("%s_%sClient", serviceImplName, rpc.GoName)
			streamApi := fmt.Sprintf("%s_%sClient", serviceApiName, rpc.GoName)

			var msgReply, msgDetails *Message
			if rpc.MsgStream != nil {
				msgDetails = rpc.MsgStream
				msgReply = rpc.MsgReply
			} else {
				msgDetails = rpc.MsgReply
				msgReply = msgControlPingReply
			}

			g.P("stream, err := c.conn.NewStream(ctx)")
			g.P("if err != nil { return nil, err }")
			g.P("x := &", streamImpl, "{stream}")
			g.P("if err := x.Stream.SendMsg(in); err != nil {")
			g.P("	return nil, err")
			g.P("}")
			if rpc.MsgStream == nil {
				g.P("if err = x.Stream.SendMsg(&", msgControlPing.GoIdent, "{}); err != nil {")
				g.P("	return nil, err")
				g.P("}")
			}
			g.P("return x, nil")
			g.P("}")
			g.P()
			g.P("type ", streamApi, " interface {")
			if msgReply != msgControlPingReply {
				g.P("	Recv() (*", msgDetails.GoIdent, ", *", msgReply.GoIdent, ", error)")
			} else {
				g.P("	Recv() (*", msgDetails.GoIdent, ", error)")
			}
			g.P("	", govppApiPkg.Ident("Stream"))
			g.P("}")
			g.P()

			g.P("type ", streamImpl, " struct {")
			g.P("	", govppApiPkg.Ident("Stream"))
			g.P("}")
			g.P()

			if msgReply != msgControlPingReply {
				g.P("func (c *", streamImpl, ") Recv() (*", msgDetails.GoIdent, ", *", msgReply.GoIdent, ", error) {")
			} else {
				g.P("func (c *", streamImpl, ") Recv() (*", msgDetails.GoIdent, ", error) {")
			}
			g.P("	msg, err := c.Stream.RecvMsg()")
			if msgReply != msgControlPingReply {
				g.P("	if err != nil { return nil, nil, err }")
			} else {
				g.P("	if err != nil { return nil, err }")
			}
			g.P("	switch m := msg.(type) {")
			g.P("	case *", msgDetails.GoIdent, ":")
			if msgReply != msgControlPingReply {
				g.P("		return m, nil, nil")
			} else {
				g.P("		return m, nil")
			}
			g.P("	case *", msgReply.GoIdent, ":")
			if msgReply != msgControlPingReply {
				if retvalField := getRetvalField(msgReply); retvalField != nil {
					g.P("if err := ", retvalFieldToErr(g, "m", retvalField), "; err != nil {")
					g.P("	c.Stream.Close()")
					if msgReply != msgControlPingReply {
						g.P("	return nil, m, err")
					} else {
						g.P("	return nil, err")
					}
					g.P("}")
				}
			}
			g.P("		err = c.Stream.Close()")
			if msgReply != msgControlPingReply {
				g.P("	if err != nil { return nil, m, err }")
			} else {
				g.P("	if err != nil { return nil, err }")
			}
			if msgReply != msgControlPingReply {
				g.P("		return nil, m, ", ioPkg.Ident("EOF"))
			} else {
				g.P("		return nil, ", ioPkg.Ident("EOF"))
			}
			g.P("	default:")
			if msgReply != msgControlPingReply {
				g.P("		return nil, nil, ", fmtPkg.Ident("Errorf"), "(\"unexpected message: %T %v\", m, m)")
			} else {
				g.P("		return nil, ", fmtPkg.Ident("Errorf"), "(\"unexpected message: %T %v\", m, m)")
			}
			g.P("}")
		} else if rpc.MsgReply != nil {
			g.P("out := new(", rpc.MsgReply.GoIdent, ")")
			g.P("err := c.conn.Invoke(ctx, in, out)")
			g.P("if err != nil { return nil, err }")
			if retvalField := getRetvalField(rpc.MsgReply); retvalField != nil {
				g.P("return out, ", retvalFieldToErr(g, "out", retvalField))
			} else {
				g.P("return out, nil")
			}
		} else { // MsgReply == nil
			g.P("stream, err := c.conn.NewStream(ctx)")
			g.P("if err != nil { return err }")
			g.P("err = stream.SendMsg(in)")
			g.P("if err != nil { return err }")
			g.P("err = stream.Close()")
			g.P("if err != nil { return err }")
			g.P("return nil")
		}
		g.P("}")
		g.P()
	}

	// TODO: generate service descriptor
	/*fmt.Fprintf(w, "var %s = api.%s{\n", serviceDescName, serviceDescType)
	  fmt.Fprintf(w, "\tServiceName: \"%s\",\n", ctx.moduleName)
	  fmt.Fprintf(w, "\tHandlerType: (*%s)(nil),\n", serviceApiName)
	  fmt.Fprintf(w, "\tMethods: []api.MethodDesc{\n")
	  for _, method := range rpcs {
	  	fmt.Fprintf(w, "\t  {\n")
	  	fmt.Fprintf(w, "\t    MethodName: \"%s\",\n", method.Name)
	  	fmt.Fprintf(w, "\t  },\n")
	  }
	  fmt.Fprintf(w, "\t},\n")
	  //fmt.Fprintf(w, "\tCompatibility: %s,\n", messageCrcName)
	  //fmt.Fprintf(w, "\tMetadata: reflect.TypeOf((*%s)(nil)).Elem().PkgPath(),\n", serviceApiName)
	  fmt.Fprintf(w, "\tMetadata: \"%s\",\n", ctx.inputFile)
	  fmt.Fprintln(w, "}")*/

	g.P()
}

func retvalFieldToErr(g *GenFile, varName string, retvalField *Field) string {
	if getFieldType(g, retvalField) == "int32" {
		return g.GoIdent(govppApiPkg.Ident("RetvalToVPPApiError")) + "(" + varName + "." + retvalField.GoName + ")"
	} else {
		return g.GoIdent(govppApiPkg.Ident("RetvalToVPPApiError")) + "(int32(" + varName + "." + retvalField.GoName + "))"
	}
}

func rpcMethodSignature(g *GenFile, rpc *RPC) string {
	s := rpc.GoName + "(ctx " + g.GoIdent(contextPkg.Ident("Context"))
	s += ", in *" + g.GoIdent(rpc.MsgRequest.GoIdent) + ") ("
	if rpc.VPP.Stream {
		s += serviceApiName + "_" + rpc.GoName + "Client, "
	} else if rpc.MsgReply != nil {
		s += "*" + g.GoIdent(rpc.MsgReply.GoIdent) + ", "
	}
	s += "error)"
	return s
}

func rpcMethodSignatureWatch(g *GenFile, rpc *RPC) string {
	s := "Watch" + rpc.GoName + "(ctx " + g.GoIdent(contextPkg.Ident("Context")) + ") ("
	streamImpl := fmt.Sprintf("%s_%sClient", serviceImplName, rpc.GoName)
	s += "*" + streamImpl + ", error)"
	return s
}
