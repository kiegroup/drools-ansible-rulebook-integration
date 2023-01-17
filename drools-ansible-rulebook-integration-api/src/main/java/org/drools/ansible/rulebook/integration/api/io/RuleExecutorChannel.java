package org.drools.ansible.rulebook.integration.api.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.charset.StandardCharsets;

import org.drools.ansible.rulebook.integration.api.rulesengine.AsyncExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class RuleExecutorChannel {

    protected static final Logger log = LoggerFactory.getLogger(RuleExecutorChannel.class);

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

    public RuleExecutorChannel accept(AsyncExecutor asyncExecutor) {
        asyncExecutor.submit(() -> {
            try {
                Socket skt = socketChannel.accept();
                this.dataOutputStream = new DataOutputStream(skt.getOutputStream());
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

    public void write(Response response) {
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
