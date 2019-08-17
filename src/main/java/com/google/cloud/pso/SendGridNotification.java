package com.google.cloud.pso;

import com.sendgrid.*;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.workflow.WorkflowNodeState;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchActionContext;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.common.batch.action.ConditionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.Map;

/**
 * NotificationExample implementation of {@link PostAction}.
 *
 * <p>
 *   {@link PostAction} are type of plugins that are executed in the
 *   Batch pipeline at the end of execution. Irrespective of the status of
 *   the pipeline this plugin will be invoked.
 *
 *   This type of plugin can be used to send notifications to external
 *   system or notify other workflows.
 * </p>
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name("Send Grid Notification")
@Description("An example of notification plugin.")
public final class SendGridNotification extends PostAction {
    private static final Logger LOG = LoggerFactory.getLogger(SendGridNotification.class);

    private final Config config;

    public static class Config extends ConditionConfig {
        @Description("From email id.")
        @Name("from email")
        @Macro
        private String fromEmail;
        @Description("To email id.")
        @Name("to email")
        @Macro
        private String toEmail;
        @Description("API key.")
        @Name("API key")
        @Macro
        private String apiKey;

        @Description("Subject.")
        @Name("subject")
        @Macro
        private String subject;

        @Description("Body.")
        @Name("body")
        @Macro
        private String body;

    }

    public SendGridNotification(Config config) {
        this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer configurer) {
        super.configurePipeline(configurer);
    }

    @Override
    public void run(BatchActionContext context) throws Exception {
        // Framework provides the ability to decide within this plugin
        // whether this should be run or no. This happens depending on
        // the status of pipeline selected -- COMPLETION, SUCCESS or FAILURE.
        if (!config.shouldRun(context)) {
            return;
        }

        WorkflowToken token = context.getToken();

        // true if SUCCESS, false otherwise.
        boolean status = context.isSuccessful();

        String subject;

        if (status) {
            subject = "Pipeline succeded. " + config.subject;
        } else {
            subject = "Pipeline failed. " + config.subject;
        }

        String body = config.body;

        Email from = new Email(config.fromEmail);
        Email to = new Email(config.toEmail);
        Content content = new Content("text/plain", body);
        Mail mail = new Mail();
        Personalization personalization = new Personalization();
        for(String toEmail : config.toEmail.split(";")){
            personalization.addTo(new Email(toEmail));
        }
        mail.setFrom(from);
        mail.setSubject(subject);
        mail.addContent(content);
        mail.addPersonalization(personalization);
        LOG.info("Sending email notification. From: " + config.fromEmail + ". To: " + config.toEmail);

        SendGrid sg = new SendGrid(config.apiKey);
        Request request = new Request();

        try {
            request.setMethod(Method.POST);
            request.setEndpoint("mail/send");
            request.setBody(mail.build());
            Response response = sg.api(request);
            System.out.println(response.getStatusCode());
            System.out.println(response.getBody());
            System.out.println(response.getHeaders());
        } catch (IOException ex) {
            throw ex;
        }

        // Extracts the status for each nodes in the dag.
        Map<String, WorkflowNodeState> nodeStates = context.getNodeStates();

    }
}
