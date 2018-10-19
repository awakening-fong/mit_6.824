

本仓库存放MIT 6.824 2018课程中关于raft的lab.


测试通过:

 * 2A
 * 2B

未进行2C.

未针对go test -race -run xxx 进行修改, 故不通过.

另外, 由于Kill()编写不完善, 需要逐个测试, 
也就是需要写成:
GOPATH=/opt/mit_6.824/ ;export GOPATH; \
go test -run BasicAgree2B && \
go test -run FailAgree2B  && \
go test -run FailNoAgree2B  && \
go test -run ConcurrentStarts2B  && \
go test -run Rejoin2B && \
go test -run Backup2B && \
go test -run Count2B


而不能写成:
GOPATH=/opt/mit_6.824/ ;export GOPATH;go test -run 2B
