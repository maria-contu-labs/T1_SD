package main

import (
	"SD/DIMEX"
	"fmt"
	"os"
	"strconv"
	"time"
)

const numberOfSnapshots = 300

func main() {

	if len(os.Args) < 3 {

		fmt.Println(
			"Uso: go run useDIMEX-snapshot.go <id> <enderecos...>",
		)

		return
	}

	id, err :=
		strconv.Atoi(
			os.Args[1],
		)

	if err != nil {
		fmt.Println("ID inválido.")
		return
	}

	addresses :=
		os.Args[2:]

	if id < 0 ||
		id >= len(addresses) {

		fmt.Println("ID inválido.")
		return
	}

	// limpa o snapshot anterior desse processo
	os.Remove(
		DIMEX.SnapshotFileName(
			id,
		),
	)

	dmx :=
		DIMEX.NewDIMEX(
			addresses,
			id,
			false,
		)

	file, err :=
		os.OpenFile(
			"./mxOUT-snapshot.txt",

			os.O_APPEND|
				os.O_CREATE|
				os.O_WRONLY,

			0644,
		)

	if err != nil {

		fmt.Println(
			"Erro ao abrir arquivo:",
			err,
		)

		return
	}

	defer file.Close()

	fmt.Printf(
		"P%d iniciado. Aguardando os outros processos...\n",
		id,
	)

	// tempo para subir os 3 processos
	time.Sleep(
		10 * time.Second,
	)

	// P0 inicia os snapshots enquanto continua usando o DiMEx
	if id == 0 {

		go func() {

			for snapshotID :=
				1; snapshotID <= numberOfSnapshots; snapshotID++ {

				dmx.SnapshotReq <- snapshotID

				time.Sleep(
					20 * time.Millisecond,
				)
			}

			fmt.Printf(
				"\nP0 solicitou %d snapshots.\n",
				numberOfSnapshots,
			)

			fmt.Println(
				"Deixe os processos rodarem mais alguns segundos antes de encerrar.",
			)
		}()
	}

	for {

		dmx.Req <- DIMEX.ENTER

		<-dmx.Ind

		// seção crítica
		_, err =
			file.WriteString("|")

		if err != nil {
			fmt.Println(err)
			return
		}

		_, err =
			file.WriteString(".")

		if err != nil {
			fmt.Println(err)
			return
		}

		dmx.Req <- DIMEX.EXIT
	}
}
