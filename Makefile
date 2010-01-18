include $(GOROOT)/src/Make.$(GOARCH)
 
TARG=net/sphinx
GOFILES=sphinx.go
 
include $(GOROOT)/src/Make.pkg

main: package
	$(GC) -I_obj main.go
	$(LD) -L_obj -o $@ main.$O

format:
	gofmt -spaces=true -tabindent=false -tabwidth=4 -w sphinx.go
	gofmt -spaces=true -tabindent=false -tabwidth=4 -w main.go
