//   SparkleShare, a collaboration and sharing tool.
//   Copyright (C) 2010  Hylke Bons <hylkebons@gmail.com>
//
//   This program is free software: you can redistribute it and/or modify
//   it under the terms of the GNU Lesser General Public License as 
//   published by the Free Software Foundation, either version 3 of the 
//   License, or (at your option) any later version.
//
//   This program is distributed in the hope that it will be useful,
//   but WITHOUT ANY WARRANTY; without even the implied warranty of
//   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//   GNU General Public License for more details.
//
//   You should have received a copy of the GNU General Public License
//   along with this program. If not, see <http://www.gnu.org/licenses/>.


using System;
using System.Text;
using System.Threading;

using NetMQ;

namespace SparkleLib {

    public class SparkleListenerNetMQ : SparkleListenerBase {

        private Thread thread;
        private bool is_connected  = false;
        private bool is_connecting = false;
        private DateTime last_ping = DateTime.Now;

        private NetMQContext context;
        private NetMQSocket subscriber_socket;


        public SparkleListenerNetMQ (Uri server, string folder_identifier) : base (server, folder_identifier)
        {
            this.context = NetMQContext.Create ();
        }


        public override bool IsConnected {
            get {
                return this.is_connected;
            }
        }


        public override bool IsConnecting {
            get {
                return this.is_connecting;
            }
        }


        // Starts a new thread and listens to the channel
        public override void Connect ()
        {
            this.is_connecting = true;

            this.thread = new Thread (() => {
                try {
                    this.subscriber_socket = this.context.CreateSubscriberSocket ();
                    this.subscriber_socket.Connect (
                        Server.OriginalString.Replace ("" + Server.Port, "" + Server.Port + 1)
                    );

                    this.is_connecting = false;
                    this.is_connected  = true;

                    OnConnected ();

                } catch (Exception e) {
                    this.is_connected  = false;
                    this.is_connecting = false;

                    OnDisconnected (SparkleLib.DisconnectReason.TimeOut, e.Message);
                    return;
                }

                this.last_ping = DateTime.Now;

                int i = 0;
                int timeout = 300;
                while (this.is_connected) {
                    string folder_identifier = subscriber_socket.ReceiveString (new TimeSpan (0, 0, 4));
                    string revision          = subscriber_socket.ReceiveString (new TimeSpan (0, 0, 2));

                    if (!string.IsNullOrEmpty (folder_identifier) && !string.IsNullOrEmpty (revision)) {
                        OnAnnouncement (new SparkleAnnouncement (folder_identifier, revision));
                    
                    } else {
                        if (i == timeout) {
                            if (Ping ()) {
                                this.last_ping = DateTime.Now;
                            
                            } else {
                                Disconnect(DisconnectReason.TimeOut, "Ping timeout");
                                return;
                            }
                        
                        } else {

                            // Check when the last ping occured. If it's
                            // significantly longer than our regular interval the
                            // system likely woke up from sleep and we want to
                            // simulate a disconnect
                            int sleepiness = DateTime.Compare (
                                this.last_ping.AddMilliseconds (timeout * 1000 * 1.2),
                                DateTime.Now
                            );

                            if (sleepiness <= 0) {
                                SparkleLogger.LogInfo ("ListenerNetMQ", "System woke up from sleep");
                                Disconnect (DisconnectReason.SystemSleep, "Ping timeout");
                            }
                        }
                    }

                    i += 6;
                }
            });

            this.thread.Start ();
        }


        private bool Ping ()
        {
            SparkleLogger.LogInfo ("ListenerNetMQ", "Pinging " + Server);
            NetMQSocket request_socket = this.context.CreateRequestSocket ();

            request_socket.Connect (Server.OriginalString);
            request_socket.Send ("ping");
            string ping_reply = request_socket.ReceiveString (new TimeSpan (0, 0, 4));
            request_socket.Dispose ();

            if (string.IsNullOrEmpty (ping_reply)) {
                return false;
            
            } else {
                SparkleLogger.LogInfo ("ListenerNetMQ", "Received pong from " + Server);
                return true;
            }
        }


        private void Disconnect (DisconnectReason reason, string message)
        {
            this.is_connected  = false;
            this.is_connecting = false;

            if (this.subscriber_socket != null) {
                this.subscriber_socket.Dispose ();
                this.subscriber_socket = null;
            }

            OnDisconnected (reason, message);
        }


        protected override void AlsoListenToInternal (string folder_identifier)
        {
            try {
                this.subscriber_socket.Subscribe (folder_identifier);
                this.last_ping = DateTime.Now;

            } catch (Exception e) {
                this.is_connected  = false;
                this.is_connecting = false;

                OnDisconnected (DisconnectReason.TimeOut, e.Message);
            }
        }


        protected override void AnnounceInternal (SparkleAnnouncement announcement)
        {
            string to_send = "announce " + announcement.FolderIdentifier + " " + announcement.Message;

            try {
                NetMQSocket request_socket = this.context.CreateRequestSocket ();

                request_socket.Connect (Server.OriginalString);
                request_socket.Send (to_send);
                request_socket.ReceiveString (new TimeSpan (0, 0, 4));
                request_socket.Dispose ();

                this.last_ping = DateTime.Now;

            } catch (Exception e) {
                this.is_connected  = false;
                this.is_connecting = false;

                OnDisconnected (DisconnectReason.TimeOut, e.Message);
            }
        }


        public override void Dispose ()
        {
            this.context.Dispose ();

            this.thread.Abort ();
            this.thread.Join ();

            base.Dispose ();
        }
    }
}
