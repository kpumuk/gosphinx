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
        for i := 0; i < len(status); i++ {
            fmt.Printf("%20s  %s\n", status[i][0]+":", status[i][1])
        }
    }
}
