package main

import (
    "net/sphinx"
    "fmt"
)

func main() {
    sphinx := sphinx.NewClient()
    sphinx.SetServer("127.0.0.1", 3312)

    fmt.Println("Retrieving Sphinx status")
    status, err := sphinx.Status()
    if err != nil {
        fmt.Println("Error: ", err)
    } else {
        for _, row := range status {
            fmt.Printf("%20s  %s\n", row[0]+":", row[1])
        }
    }
}
