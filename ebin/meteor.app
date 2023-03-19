{application, gen_echo_server,
 [
  {description, "A PubSub app for Orbital"},
  {vsn, "0.0.1"},
  {modules, [
             meteor_conn_server,
             meteor_cli,
             meteor_node_manager,
             meteor_node_sync,
             meteor_stream_manager,
             meteor_stream_sup,
             meteor_stream,
             meteor_sup,
             meteor
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {env, []}
 ]}.
