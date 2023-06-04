package org.drools.ansible.rulebook.integration.api.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.drools.ansible.rulebook.integration.api.rulesengine.AsyncExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class RuleExecutorChannel {

    protected static final Logger log = LoggerFactory.getLogger(RuleExecutorChannel.class);

    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName("drools-channel-accept-thread");
        return t;
    });

    private final ServerSocket socketChannel;
    private volatile DataOutputStream dataOutputStream;

    private volatile boolean connected = false;

    public RuleExecutorChannel() {
        try {
            socketChannel = new ServerSocket(0); // 0 means kernel will choose a free port
            socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int port() {
        // used by client to know on which port the socket has been opened
        return socketChannel.getLocalPort();
    }

    public RuleExecutorChannel accept() {
        executor.submit(() -> {
            try {
                if (log.isInfoEnabled()) {
                    log.info("Waiting for the async channel to connect");
                }

                Socket skt = socketChannel.accept();
                this.dataOutputStream = new DataOutputStream(skt.getOutputStream());

//                try {
//                    Thread.sleep(1300);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }

                this.connected = true;

                if (log.isInfoEnabled()) {
                    log.info("Async channel connected");
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        return this;
    }

    public boolean isConnected() {
        for (int i = 0; !connected && i < 100; i++) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return connected;
    }

    public byte[] write(Response response) {
        try {
            String payload = toJson(response);

            if (log.isInfoEnabled()) {
                log.info("Writing payload on the async channel: " + payload);
            }

            byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
            dataOutputStream.writeInt(bytes.length);
            dataOutputStream.write(bytes);
            dataOutputStream.flush();

            if (log.isInfoEnabled()) {
                log.info(bytes.length  + " bytes have been written on the async channel");
            }

            return bytes;
        } catch (IOException | UncheckedIOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        try {
            if (dataOutputStream != null) {
                dataOutputStream.close();
            }
            executor.shutdown();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            try {
                socketChannel.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
