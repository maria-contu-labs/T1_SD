package DIMEX

import (
	PP2PLink "SD/PP2PLink"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type State int

const (
	noMX State = iota
	wantMX
	inMX
)

type dmxReq int

const (
	ENTER dmxReq = iota
	EXIT
)

type dmxResp struct{}

// estado salvo de cada processo
type SnapshotLocalState struct {
	State    string `json:"state"`
	Lcl      int    `json:"lcl"`
	ReqTs    int    `json:"reqTs"`
	NbrResps int    `json:"nbrResps"`
	Waiting  []bool `json:"waiting"`
}

type SnapshotMessage struct {
	From    int    `json:"from"`
	Message string `json:"message"`
}

type SnapshotRecord struct {
	SnapshotID int                       `json:"snapshotId"`
	ProcessID  int                       `json:"processId"`
	LocalState SnapshotLocalState        `json:"localState"`
	Channels   map[int][]SnapshotMessage `json:"channels"`
}

type snapshotContext struct {
	id int

	localState SnapshotLocalState

	recording map[int]bool

	markerReceived map[int]bool

	channels map[int][]SnapshotMessage
}

type DIMEX_Module struct {
	Req chan dmxReq
	Ind chan dmxResp

	addresses []string
	id        int

	st State

	waiting []bool

	lcl   int
	reqTs int

	nbrResps int

	dbg bool

	Pp2plink *PP2PLink.PP2PLink

	SnapshotReq chan int

	snapshots map[int]*snapshotContext
}

func NewDIMEX(
	_addresses []string,
	_id int,
	_dbg bool,
) *DIMEX_Module {

	p2p :=
		PP2PLink.NewPP2PLink(
			_addresses[_id],
			_dbg,
		)

	dmx :=
		&DIMEX_Module{
			Req: make(
				chan dmxReq,
				1,
			),

			Ind: make(
				chan dmxResp,
				1,
			),

			addresses: _addresses,
			id:        _id,

			st: noMX,

			waiting: make(
				[]bool,
				len(_addresses),
			),

			lcl:   0,
			reqTs: 0,

			nbrResps: 0,

			dbg: _dbg,

			Pp2plink: p2p,

			SnapshotReq: make(
				chan int,
				1,
			),

			snapshots: make(
				map[int]*snapshotContext,
			),
		}

	for i :=
		0; i < len(dmx.waiting); i++ {

		dmx.waiting[i] = false
	}

	dmx.Start()

	dmx.outDbg(
		"Init DIMEX!",
	)

	return dmx
}

func (module *DIMEX_Module) Start() {

	go func() {

		for {

			select {

			case dmxR :=
				<-module.Req:

				if dmxR == ENTER {

					module.outDbg(
						"app pede mx",
					)

					module.
						handleUponReqEntry()

				} else if dmxR == EXIT {

					module.outDbg(
						"app libera mx",
					)

					module.
						handleUponReqExit()
				}

			case snapshotID :=
				<-module.SnapshotReq:

				module.
					handleSnapshotRequest(
						snapshotID,
					)

			case msgOutro :=
				<-module.Pp2plink.Ind:

				if strings.HasPrefix(
					msgOutro.Message,
					"marker||",
				) {

					module.
						handleMarker(
							msgOutro,
						)

					continue
				}

				module.
					recordSnapshotMessage(
						msgOutro,
					)

				if strings.Contains(
					msgOutro.Message,
					"respOK",
				) {

					module.outDbg(
						"<<<---- responde! " +
							msgOutro.Message,
					)

					module.
						handleUponDeliverRespOk(
							msgOutro,
						)

				} else if strings.Contains(
					msgOutro.Message,
					"reqEntry",
				) {

					module.outDbg(
						"<<<---- pede?? " +
							msgOutro.Message,
					)

					module.
						handleUponDeliverReqEntry(
							msgOutro,
						)
				}
			}
		}
	}()
}

func (module *DIMEX_Module) handleUponReqEntry() {

	module.lcl++

	module.reqTs =
		module.lcl

	module.nbrResps = 0

	for i, address := range module.addresses {

		if i == module.id {
			continue
		}

		module.sendToLink(
			address,
			fmt.Sprintf(
				"reqEntry||%d||%d",
				module.id,
				module.reqTs,
			),
			"     ",
		)
	}

	module.st = wantMX
}

func (module *DIMEX_Module) handleUponReqExit() {

	for i, address := range module.addresses {

		if !module.waiting[i] {
			continue
		}

		module.sendToLink(
			address,
			fmt.Sprintf(
				"respOK||%d||%d",
				module.id,
				module.lcl,
			),
			"     ",
		)

		module.waiting[i] = false
	}

	module.st = noMX
}

func (
	module *DIMEX_Module,
) handleUponDeliverRespOk(
	msgOutro PP2PLink.PP2PLink_Ind_Message,
) {

	module.nbrResps++

	module.outDbg(
		fmt.Sprintf(
			"respOK %d/%d",
			module.nbrResps,
			len(module.addresses)-1,
		),
	)

	if module.nbrResps ==
		len(module.addresses)-1 {

		module.st = inMX

		module.Ind <- dmxResp{}
	}
}

func (
	module *DIMEX_Module,
) handleUponDeliverReqEntry(
	msgOutro PP2PLink.PP2PLink_Ind_Message,
) {

	parts :=
		strings.Split(
			msgOutro.Message,
			"||",
		)

	if len(parts) != 3 {

		module.outDbg(
			"reqEntry inválido: " +
				msgOutro.Message,
		)

		return
	}

	senderID, err :=
		strconv.Atoi(
			parts[1],
		)

	if err != nil {
		return
	}

	senderTs, err :=
		strconv.Atoi(
			parts[2],
		)

	if err != nil {
		return
	}

	if module.st == noMX ||
		(module.st == wantMX &&
			after(
				module.id,
				module.reqTs,
				senderID,
				senderTs,
			)) {

		module.sendToLink(
			module.addresses[senderID],

			fmt.Sprintf(
				"respOK||%d||%d",
				module.id,
				module.lcl,
			),

			"     ",
		)

	} else {

		module.waiting[senderID] =
			true
	}

	module.lcl =
		max(
			module.lcl,
			senderTs,
		)
}

// inicia um snapshot local
func (
	module *DIMEX_Module,
) handleSnapshotRequest(
	snapshotID int,
) {

	if _, exists :=
		module.snapshots[snapshotID]; exists {

		return
	}

	module.outDbg(
		fmt.Sprintf(
			"INICIA SNAPSHOT %d",
			snapshotID,
		),
	)

	context :=
		module.
			newSnapshotContext(
				snapshotID,
			)

	module.snapshots[snapshotID] =
		context

	module.
		sendMarkers(
			snapshotID,
		)
}

func (
	module *DIMEX_Module,
) newSnapshotContext(
	snapshotID int,
) *snapshotContext {

	waitingCopy :=
		make(
			[]bool,
			len(module.waiting),
		)

	copy(
		waitingCopy,
		module.waiting,
	)

	context :=
		&snapshotContext{
			id: snapshotID,

			localState: SnapshotLocalState{
				State: stateToString(
					module.st,
				),

				Lcl: module.lcl,

				ReqTs: module.reqTs,

				NbrResps: module.nbrResps,

				Waiting: waitingCopy,
			},

			recording: make(
				map[int]bool,
			),

			markerReceived: make(
				map[int]bool,
			),

			channels: make(
				map[int][]SnapshotMessage,
			),
		}

	for i := range module.addresses {

		if i == module.id {
			continue
		}

		context.recording[i] =
			true

		context.markerReceived[i] =
			false

		context.channels[i] =
			[]SnapshotMessage{}
	}

	return context
}

func (
	module *DIMEX_Module,
) sendMarkers(
	snapshotID int,
) {

	for i, address := range module.addresses {

		if i == module.id {
			continue
		}

		module.sendToLink(
			address,

			fmt.Sprintf(
				"marker||%d||%d",
				snapshotID,
				module.id,
			),

			" SNAP ",
		)
	}
}

// trata os markers do Chandy-Lamport
func (
	module *DIMEX_Module,
) handleMarker(
	msgOutro PP2PLink.PP2PLink_Ind_Message,
) {

	parts :=
		strings.Split(
			msgOutro.Message,
			"||",
		)

	if len(parts) != 3 {
		return
	}

	snapshotID, err :=
		strconv.Atoi(
			parts[1],
		)

	if err != nil {
		return
	}

	senderID, err :=
		strconv.Atoi(
			parts[2],
		)

	if err != nil {
		return
	}

	context, exists :=
		module.snapshots[snapshotID]

	if !exists {

		module.outDbg(
			fmt.Sprintf(
				"PRIMEIRO MARKER SNAPSHOT %d DE P%d",
				snapshotID,
				senderID,
			),
		)

		context =
			module.
				newSnapshotContext(
					snapshotID,
				)

		module.snapshots[snapshotID] =
			context

		context.recording[senderID] =
			false

		context.markerReceived[senderID] =
			true

		module.
			sendMarkers(
				snapshotID,
			)

	} else {

		module.outDbg(
			fmt.Sprintf(
				"MARKER SNAPSHOT %d DE P%d",
				snapshotID,
				senderID,
			),
		)

		context.recording[senderID] =
			false

		context.markerReceived[senderID] =
			true
	}

	module.
		tryFinishSnapshot(
			snapshotID,
		)
}

// guarda mensagens que ainda estavam em trânsito
func (
	module *DIMEX_Module,
) recordSnapshotMessage(
	msgOutro PP2PLink.PP2PLink_Ind_Message,
) {

	parts :=
		strings.Split(
			msgOutro.Message,
			"||",
		)

	if len(parts) != 3 {
		return
	}

	senderID, err :=
		strconv.Atoi(
			parts[1],
		)

	if err != nil {
		return
	}

	for _, context := range module.snapshots {

		if context.recording[senderID] {

			context.channels[senderID] =
				append(
					context.channels[senderID],

					SnapshotMessage{
						From: senderID,

						Message: msgOutro.Message,
					},
				)
		}
	}
}

// finaliza quando chegaram markers de todos os canais
func (
	module *DIMEX_Module,
) tryFinishSnapshot(
	snapshotID int,
) {

	context, exists :=
		module.snapshots[snapshotID]

	if !exists {
		return
	}

	for i := range module.addresses {

		if i == module.id {
			continue
		}

		if !context.markerReceived[i] {

			return
		}
	}

	module.
		saveSnapshot(
			context,
		)

	delete(
		module.snapshots,
		snapshotID,
	)

	module.outDbg(
		fmt.Sprintf(
			"FINALIZA SNAPSHOT %d",
			snapshotID,
		),
	)
}

func (
	module *DIMEX_Module,
) saveSnapshot(
	context *snapshotContext,
) {

	record :=
		SnapshotRecord{
			SnapshotID: context.id,

			ProcessID: module.id,

			LocalState: context.localState,

			Channels: context.channels,
		}

	file, err :=
		os.OpenFile(
			SnapshotFileName(
				module.id,
			),

			os.O_APPEND|
				os.O_CREATE|
				os.O_WRONLY,

			0644,
		)

	if err != nil {

		fmt.Println(
			"Erro ao abrir arquivo de snapshot:",
			err,
		)

		return
	}

	defer file.Close()

	encoder :=
		json.NewEncoder(
			file,
		)

	err =
		encoder.Encode(
			record,
		)

	if err != nil {

		fmt.Println(
			"Erro ao gravar snapshot:",
			err,
		)
	}
}

func SnapshotFileName(
	processID int,
) string {

	return fmt.Sprintf(
		"snapshots_p%d.jsonl",
		processID,
	)
}

func stateToString(
	state State,
) string {

	switch state {

	case noMX:
		return "noMX"

	case wantMX:
		return "wantMX"

	case inMX:
		return "inMX"

	default:
		return "unknown"
	}
}

func (
	module *DIMEX_Module,
) sendToLink(
	address string,
	content string,
	space string,
) {

	module.outDbg(
		space +
			" ---->>>> to: " +
			address +
			" msg: " +
			content,
	)

	module.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
		To:      address,
		Message: content,
	}
}

func before(
	oneId int,
	oneTs int,
	othId int,
	othTs int,
) bool {

	if oneTs < othTs {
		return true
	}

	if oneTs > othTs {
		return false
	}

	return oneId < othId
}

func after(
	oneId int,
	oneTs int,
	othId int,
	othTs int,
) bool {

	return before(
		othId,
		othTs,
		oneId,
		oneTs,
	)
}

func max(
	a int,
	b int,
) int {

	if a > b {
		return a
	}

	return b
}

func (
	module *DIMEX_Module,
) outDbg(
	s string,
) {

	if module.dbg {

		fmt.Println(
			". . . . . . . . . . . . [ DIMEX : " +
				s +
				" ]",
		)
	}
}
