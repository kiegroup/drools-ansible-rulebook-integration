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

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class RuleExecutorChannel {
    private final ServerSocket socketChannel;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile DataOutputStream dataOutputStream;

    public RuleExecutorChannel() {
        try {
            socketChannel = new ServerSocket(0); // 0 means kernel will choose a free port
            socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            accept();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int port() {
        // used by client to know on which port the socket has been opened
        return socketChannel.getLocalPort();
    }

    private void accept() {
        executor.submit(() -> {
            try {
                Socket skt = socketChannel.accept();
                this.dataOutputStream = new DataOutputStream(skt.getOutputStream());
                return skt;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void write(Response response) {
        executor.submit(() -> {
            try {
                String payload = toJson(response);
                byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
                dataOutputStream.writeInt(bytes.length);
                dataOutputStream.write(bytes);
                dataOutputStream.flush();
            } catch (IOException | UncheckedIOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void shutdown() {
        executor.shutdown();
    }
}
