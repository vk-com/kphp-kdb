<?php


/*
Автор: Андрей Гоглев [https://vk.com/id125864255]

Предпологается что у вас уже настроен Queue Engine и установлен nginx

В конфиги nginx нужно добавить

location ~ "^/im(.*)$" {
	proxy_set_header X-Real-IP $remote_addr;
	proxy_pass http://localhost:8888;
}

location "/q_frame.php" {
	proxy_redirect off;
	proxy_set_header X-Real-IP $remote_addr;
	proxy_pass http://localhost:8080;
}

и в fastcgi_params добавить
fastcgi_param 	HTTP_X_REAL_IP 	$remote_addr;


Движок должен быть запущен с HTTP портом 3311 а KPHP вебсервер на 8888 или же измените их


Для тестов нужно открыть две вкладки браузера

Одна http://example.com/q_frame.php
Вторая http://example.com/q_frame.php?mode=1

В первой нажать Start
Во воторой отправляйте сообщения

В первой в консоле должны выводиться результаты

Если не работает значит что то не так сделалил

*/


$act = $_POST['act'];

$queue = new Memcache;
$queue->connect('127.0.0.1', 11088) or die('unavailable service');

switch($act){

    case 'add':
        $text = $_POST['str'];
        $qname = 'project1';
        $queue->add("queue({$qname})", $text);
        break;

    default:

        $mode = $_GET['mode'];

        if($mode){
            $actions = '<input type="text" placeholder="Event text" id="event_text"/><br/><button onClick="addEvent();">Send</button>';
        }else{
            $user_id = 1;
            $ip = ip2long($_SERVER['HTTP_X_REAL_IP']);
            $wait = 25;//max 120
            $qname = 'project1';
            $queue->get("upd_secret{$user_id}"); //remove old keys
            $data = $queue->get("timestamp_key{$user_id},{$ip},{$wait}({$qname})");
            $data = json_decode($data, true);

            if(!$data['ts'] || !$data['key']) die('error'); //OR не работает!

            $KEY = $data['key'];
            $TS = $data['ts'];

            $actions = '<button onClick="start();">Start poll</button>';
        }

        $html = '
<html>
	<head>
		<script type="text/javascript">
			var ajax = {
				init: function(){
					var xhr = false;
					try {
						xhr = new ActiveXObject("Msxml2.XMLHTTP");
					}catch(e){
						try {
							xhr = new ActiveXObject("Microsoft.XMLHTTP");
						}catch(E){
							xhr = false;
						}
					}
					if (!xhr && typeof XMLHttpRequest != \'undefined\') {
						xhr = new XMLHttpRequest();
					}
					return xhr;
				},
				req: false,
				post: function(url, query, callback){
					var xhr = ajax.init();
					var ts = new Date().getTime(), data = \'rand=\'+ts;
					if(typeof query == \'object\'){
						for(var i in query) data += \'&\'+i+\'=\'+encodeURIComponent(query[i]);
					}else if(typeof query == \'function\') callback = query;
					xhr.onreadystatechange = function() {
						if (xhr.readyState == 4){
							if(xhr.status == 200){
								if(callback) callback(xhr.responseText);
							}
						}
					};
					xhr.open(\'POST\', url, true);
					xhr.setRequestHeader(\'Content-Type\', \'application/x-www-form-urlencoded\');
					xhr.setRequestHeader(\'X-Requested-With\', \'XMLHttpRequest\');
					xhr.send(data);
				}
			};
			    var TS = \''.$TS.'\', KEY = \''.$KEY.'\', UID = \''.$user_id.'\', WAIT = \''.$wait.'\';
			function start () {
				document.body.innerHTML = \'Waiting events.. <br>See console\';
				ajax.post(\'/im255\', {act: \'a_check\', key: KEY, ts: TS, id: UID, wait: WAIT}, function(d){
					d = JSON.parse(d);
					console.log(d);
					TS = d.ts;
					setTimeout(start, 200);
				});
			}

			function addEvent(){
				ajax.post(\'/q_frame.php\', {act: \'add\', str: document.getElementById(\'event_text\').value});
				document.getElementById(\'event_text\').value = \'\';
			}
		</script>
	</head>
	<body>
		'.$actions.'
	</body>
</html>';

        echo $html;
}