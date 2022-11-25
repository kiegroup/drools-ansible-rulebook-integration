package org.drools.ansible.rulebook.integration.api.io;

public class Response {
    private final long session_id;
    private final Object result;

    public Response(long session_id, Object result) {
        this.session_id = session_id;
        this.result = result;
    }

    public long getSession_id() {
        return session_id;
    }

    public Object getResult() {
        return result;
    }
}