touch ./tdata/a.txt
rm ./tdata/index.db
rm -f ./jdata/*
go run cmd/SurfstoreClientExec/main.go localhost:8081 ./tdata 4096 >client1.log
go run cmd/SurfstoreClientExec/main.go localhost:8081 ./jdata 4096 >client2.log
rm ./jdata/a.txt
go run cmd/SurfstoreClientExec/main.go localhost:8081 ./jdata 4096 >client2.log
go run cmd/SurfstoreClientExec/main.go localhost:8081 ./tdata 4096 >client1.log
touch ./tdata/a.txt
go run cmd/SurfstoreClientExec/main.go localhost:8081 ./tdata 4096 >client1.log
go run cmd/SurfstoreClientExec/main.go localhost:8081 ./jdata 4096 >client2.log

#rm ./tdata/a.txt
#go run cmd/SurfstoreClientExec/main.go localhost:8081 ./tdata 4096 >client1.log

#go run cmd/SurfstoreClientExec/main.go localhost:8080 ./tdata 4096 >client1.log
#go run cmd/SurfstoreClientExec/main.go localhost:8080 ./jdata 4096 >client2.log

#touch ./tdata/5.txt
#echo "5a" >> ./tdata/5.txt
#touch ./jdata/6j.txt
#go run cmd/SurfstoreClientExec/main.go localhost:8081 ./jdata 4096 >client2.log
#go run cmd/SurfstoreClientExec/main.go localhost:8081 ./tdata 4096 >client1.log