GOPATH=/opt/tmp/mit_6.824/ ;export GOPATH;\
go test -run BasicAgree2B && \
go test -run FailAgree2B  && \
go test -run FailNoAgree2B  && \
go test -run ConcurrentStarts2B  && \
go test -run Rejoin2B && \
go test -run Backup2B && \
go test -run Count2B


