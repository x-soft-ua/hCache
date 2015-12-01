/**
 * Simple hot-cache client
 * 
 * @author Stas Oreshin orstse@gmail.com
 */

//Модули
var http = require("http");
var url = require('url');

//Редис, локальный кеш
var redis = require("/usr/local/nodejs/node_modules/redis"),
client = redis.createClient(4910, '127.0.0.1', {})

client.on("error", function (err) {
      console.log("Error " + err);
  });
 
var new_count = 0;
var cached_count = 0;
var queued_count = 0;
var new_catched_count = 0;

var events_count = 0;
var max_events = 5000;


function onRequest(request, response) {

    client_connection_closed = 0;
    
    var url = require('url');
    var url_parts = url.parse(request.url, true);
    var query = url_parts.query;
  
    if(typeof query.stat !== 'undefined')
    {
        return_json = {};
        return_json.new = new_count;
        return_json.new_catched_count = new_catched_count;
        return_json.cache = cached_count;
        return_json.queued = queued_count;
        return_json.curr_events = events_count;
        
        return_json.new_index = new_count/(new_count+cached_count);
        return_json.cache_index = cached_count/(new_count+cached_count);
        
        response.writeHead(200, {"Content-Type": "text/plain"});
        response.write(JSON.stringify(return_json));
        response.end();
        
        client_connection_closed = 1;
    }
    
    if(typeof query.uid == 'string' && client_connection_closed == 0)
    {
        var user_id = query.uid
        client.get(user_id, function (err, reply) {
                if (typeof reply == "string" && (typeof query.update == "undefined")) {
                    response.writeHead(200, {"Content-Type": "text/plain"});
                    response.write(reply);
                    response.end();
                    cached_count++;
                    client_connection_closed = 1;
                    reply = null;
                }
                else if((typeof query.update !== "undefined") || typeof reply !== "string")
                {
                    new_count++;
                    if (events_count<=max_events) {
                        setTimeout(function(){
                            if (client_connection_closed!==1) {
                                queued_count++;
                                response.writeHead(200, {"Content-Type": "text/plain"});
                                response.write('{"client_status":"queued"}');
                                response.end();
                                client_connection_closed = 1;
                            }
                        }, 10)
                        
                        events_count++;
                        //Основной запрос к службе которую кешируем
                        var request_http = http.get("http://127.0.0.1:89/get?get=" + user_id + "&only_uid=" + user_id, function(res) {
                            
                            var body = '';
                            res.on('data', function(d) {
                                body += d;
                            });
                            res.on('end', function() {
                                events_count--;
                                client_connection_closed = 1;
                                response.writeHead(200, {"Content-Type": "text/plain"});
                                response.write(body);
                                response.end();
                                client.setex(user_id, 3600*3, body);
                                new_catched_count++;
                                body = null;
                             });
                        }).on('error', function(e) {
                            events_count--;
                            client_connection_closed = 1;
                            response.writeHead(200, {"Content-Type": "text/plain"});
                            response.write('{"client_status":"error"}');
                            response.end();
                        });
                        request_http.end();
                    }
                    else
                    {
                        client_connection_closed = 1;
                        response.writeHead(200, {"Content-Type": "text/plain"});
                        response.write('{"client_status":"event_list_full"}');
                        response.end();                       
                    }

                    
                    
                }
            });
        
    }
    else
    {
        if (client_connection_closed!==1) {
            response.writeHead(200, {"Content-Type": "text/plain"});
            response.write('{"client_status":"uid_empty"}');
            response.end();
            client_connection_closed = 1
        }
    }
}



http.createServer(onRequest).listen(89);
http.createServer(onRequest).listen('/var/run/hCache.sock');

console.log("Server has started.");


