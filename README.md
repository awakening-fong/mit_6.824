

本仓库存放MIT 6.824 2018课程中关于raft的lab.


测试通过:

 * TestInitialElection2A 
 * TestReElection2A  
 * TestBasicAgree2B 
 * TestFailAgree2B 
 * TestFailNoAgree2B 
 * TestConcurrentStarts2B 
 * TestRejoin2B 
 * TestCount2B

测试失败:
 * TestBackup2B

未进行2C.

未针对go test -race -run xxx 进行修改, 故不通过.

另外, 由于Kill()编写不完善, 需要逐个测试, 
也就是需要写成:
GOPATH=/opt/mit_6.824/ ;export GOPATH;go test -run BasicAgree2B && \
go test -run FailAgree2B  && \
go test -run FailNoAgree2B 

而不能写成:
GOPATH=/opt/mit_6.824/ ;export GOPATH;go test -run 2B
