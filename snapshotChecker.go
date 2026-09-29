package main

import (
	"SD/DIMEX"
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

type GlobalSnapshot map[int]DIMEX.SnapshotRecord

func main() {

	files :=
		[]string{
			"snapshots_p0.jsonl",
			"snapshots_p1.jsonl",
			"snapshots_p2.jsonl",
		}

	snapshots :=
		make(
			map[int]GlobalSnapshot,
		)

	for _, fileName := range files {

		err :=
			loadSnapshots(
				fileName,
				snapshots,
			)

		if err != nil {

			fmt.Println(
				"Erro:",
				err,
			)

			return
		}
	}

	ids :=
		make(
			[]int,
			0,
			len(snapshots),
		)

	for id := range snapshots {

		ids =
			append(
				ids,
				id,
			)
	}

	sort.Ints(ids)

	complete := 0
	valid := 0
	invalid := 0
	incomplete := 0

	for _, snapshotID := range ids {

		snapshot :=
			snapshots[snapshotID]

		if len(snapshot) != len(files) {

			incomplete++

			fmt.Printf(
				"Snapshot %d INCOMPLETO (%d/%d processos)\n",
				snapshotID,
				len(snapshot),
				len(files),
			)

			continue
		}

		complete++

		violations :=
			checkSnapshot(
				snapshot,
			)

		if len(violations) == 0 {

			valid++

		} else {

			invalid++

			fmt.Printf(
				"\nSnapshot %d: VIOLAÇÃO\n",
				snapshotID,
			)

			for _, violation := range violations {

				fmt.Println(
					" -",
					violation,
				)
			}
		}
	}

	fmt.Println(
		"\n==============================",
	)

	fmt.Println(
		"RESULTADO DOS SNAPSHOTS",
	)

	fmt.Println(
		"==============================",
	)

	fmt.Println(
		"Snapshots encontrados:",
		len(ids),
	)

	fmt.Println(
		"Snapshots completos:",
		complete,
	)

	fmt.Println(
		"Snapshots válidos:",
		valid,
	)

	fmt.Println(
		"Snapshots inválidos:",
		invalid,
	)

	fmt.Println(
		"Snapshots incompletos:",
		incomplete,
	)

	if complete > 0 &&
		invalid == 0 {

		fmt.Println(
			"\nRESULTADO: invariantes preservadas.",
		)

	} else {

		fmt.Println(
			"\nRESULTADO: foram encontradas violações.",
		)
	}
}

func loadSnapshots(
	fileName string,
	snapshots map[int]GlobalSnapshot,
) error {

	file, err :=
		os.Open(
			fileName,
		)

	if err != nil {
		return err
	}

	defer file.Close()

	scanner :=
		bufio.NewScanner(
			file,
		)

	for scanner.Scan() {

		var record DIMEX.SnapshotRecord

		err :=
			json.Unmarshal(
				scanner.Bytes(),
				&record,
			)

		if err != nil {
			return err
		}

		if snapshots[record.SnapshotID] ==
			nil {

			snapshots[record.SnapshotID] =
				make(
					GlobalSnapshot,
				)
		}

		snapshots[record.SnapshotID][record.ProcessID] =
			record
	}

	return scanner.Err()
}

func checkSnapshot(
	snapshot GlobalSnapshot,
) []string {

	violations :=
		[]string{}

	if !inv1(snapshot) {

		violations =
			append(
				violations,
				"Inv1: mais de um processo em inMX.",
			)
	}

	if !inv2(snapshot) {

		violations =
			append(
				violations,
				"Inv2: todos estão em noMX, mas existem waiting ou mensagens em trânsito.",
			)
	}

	if !inv3(snapshot) {

		violations =
			append(
				violations,
				"Inv3: existe waiting em processo que está noMX.",
			)
	}

	if !inv4(snapshot) {

		violations =
			append(
				violations,
				"Inv4: conservação das autorizações para processo em wantMX foi violada.",
			)
	}

	if !inv5(snapshot) {

		violations =
			append(
				violations,
				"Inv5: processo em inMX não possui N-1 respostas.",
			)
	}

	if !inv6(snapshot) {

		violations =
			append(
				violations,
				"Inv6: processo continua em wantMX mesmo já possuindo N-1 respostas.",
			)
	}

	if !inv7(snapshot) {

		violations =
			append(
				violations,
				"Inv7: processo possui waiting para ele mesmo.",
			)
	}

	return violations
}

// no máximo um processo pode estar na SC
func inv1(
	snapshot GlobalSnapshot,
) bool {

	inMXCount := 0

	for _, process := range snapshot {

		if process.LocalState.State ==
			"inMX" {

			inMXCount++
		}
	}

	return inMXCount <= 1
}

// se todos estão fora da SC, não pode sobrar waiting nem mensagem em trânsito
func inv2(
	snapshot GlobalSnapshot,
) bool {

	allNoMX :=
		true

	for _, process := range snapshot {

		if process.LocalState.State !=
			"noMX" {

			allNoMX =
				false

			break
		}
	}

	if !allNoMX {
		return true
	}

	for _, process := range snapshot {

		for _, waiting := range process.LocalState.Waiting {

			if waiting {
				return false
			}
		}

		for _, messages := range process.Channels {

			if len(messages) > 0 {
				return false
			}
		}
	}

	return true
}

// waiting só faz sentido se o processo quer ou está na SC
func inv3(
	snapshot GlobalSnapshot,
) bool {

	for _, process := range snapshot {

		hasWaiting :=
			false

		for _, waiting := range process.LocalState.Waiting {

			if waiting {

				hasWaiting =
					true

				break
			}
		}

		if hasWaiting &&
			process.LocalState.State ==
				"noMX" {

			return false
		}
	}

	return true
}

// para quem está em wantMX, as N-1 autorizações precisam estar contabilizadas
func inv4(
	snapshot GlobalSnapshot,
) bool {

	n :=
		len(snapshot)

	for pID, process := range snapshot {

		if process.LocalState.State !=
			"wantMX" {

			continue
		}

		total :=
			process.LocalState.NbrResps

		for qID, q := range snapshot {

			if qID == pID {
				continue
			}

			for _, message := range q.Channels[pID] {

				if strings.HasPrefix(
					message.Message,
					"reqEntry||",
				) {

					total++
				}
			}
		}

		for senderID, messages := range process.Channels {

			if senderID == pID {
				continue
			}

			for _, message := range messages {

				if strings.HasPrefix(
					message.Message,
					"respOK||",
				) {

					total++
				}
			}
		}

		for qID, q := range snapshot {

			if qID == pID {
				continue
			}

			if pID <
				len(q.LocalState.Waiting) &&
				q.LocalState.Waiting[pID] {

				total++
			}
		}

		if total != n-1 {
			return false
		}
	}

	return true
}

// quem está em inMX já recebeu N-1 respostas
func inv5(
	snapshot GlobalSnapshot,
) bool {

	n :=
		len(snapshot)

	for _, process := range snapshot {

		if process.LocalState.State ==
			"inMX" &&
			process.LocalState.NbrResps !=
				n-1 {

			return false
		}
	}

	return true
}

// quem já recebeu N-1 respostas não deveria continuar em wantMX
func inv6(
	snapshot GlobalSnapshot,
) bool {

	n :=
		len(snapshot)

	for _, process := range snapshot {

		if process.LocalState.State ==
			"wantMX" &&
			process.LocalState.NbrResps >=
				n-1 {

			return false
		}
	}

	return true
}

// um processo nunca espera por ele mesmo
func inv7(
	snapshot GlobalSnapshot,
) bool {

	for id, process := range snapshot {

		if id <
			len(process.LocalState.Waiting) &&
			process.LocalState.Waiting[id] {

			return false
		}
	}

	return true
}
