echo "start:" >> tmp.log
echo $* >> tmp.log
date >> tmp.log
while test true;do 
echo $*
sh $*
ret=$?
if [ "$ret" != 0 ];then
break;
fi
sleep 3;
done
echo "done/fail:" >> tmp.log
echo $* >> tmp.log
date >> tmp.log
