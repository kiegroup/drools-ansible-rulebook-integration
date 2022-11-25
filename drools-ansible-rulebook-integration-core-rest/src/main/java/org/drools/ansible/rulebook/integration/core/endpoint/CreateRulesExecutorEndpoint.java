package org.drools.ansible.rulebook.integration.core.endpoint;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;

import static org.drools.ansible.rulebook.integration.api.ObjectMapperFactory.createMapper;

@Path("/create-rules-executor")
public class CreateRulesExecutorEndpoint {

    @POST()
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public long executeQuery(String s) throws JsonProcessingException {
        RulesSet rulesSet = createMapper(new JsonFactory()).readValue(s, RulesSet.class);
        return RulesExecutorContainerService.INSTANCE.register( RulesExecutorFactory.createRulesExecutor(rulesSet) ).getId();
    }
}
