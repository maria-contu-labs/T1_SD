package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {

	data, err :=
		os.ReadFile(
			"mxOUT.txt",
		)

	if err != nil {

		fmt.Println(
			"Erro ao ler mxOUT.txt:",
			err,
		)

		return
	}

	content :=
		string(data)

	doubleEnter :=
		strings.Count(
			content,
			"||",
		)

	doubleExit :=
		strings.Count(
			content,
			"..",
		)

	fmt.Println(
		"Caracteres escritos:",
		len(content),
	)

	fmt.Println(
		"Ocorrências de ||:",
		doubleEnter,
	)

	fmt.Println(
		"Ocorrências de ..:",
		doubleExit,
	)

	if doubleEnter == 0 &&
		doubleExit == 0 {

		fmt.Println(
			"RESULTADO: exclusão mútua preservada.",
		)

	} else {

		fmt.Println(
			"RESULTADO: VIOLAÇÃO da exclusão mútua.",
		)
	}
}