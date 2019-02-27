package com.jacobsonmt.ccrs.services;

import com.jacobsonmt.ccrs.model.CCRSJob;
import com.jacobsonmt.ccrs.settings.SiteSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import javax.servlet.http.HttpServletRequest;
import java.util.Objects;

@Service
public class EmailService {

    @Autowired
    private JavaMailSender emailSender;

    @Autowired
    SiteSettings siteSettings;

    private void sendMessage( String subject, String content, String to ) throws MessagingException {
        sendMessage( subject, content, to, null );
    }

    private void sendMessage( String subject, String content, String to, MultipartFile attachment ) throws MessagingException {

        MimeMessage message = emailSender.createMimeMessage();

        MimeMessageHelper helper = new MimeMessageHelper( message, true );

        helper.setSubject( subject );
        helper.setText( content, true );
        helper.setTo( to );
        helper.setFrom( siteSettings.getFromEmail() );

        if ( attachment != null ) {
            helper.addAttachment( attachment.getOriginalFilename(), attachment );
        }

        emailSender.send( message );

    }

    public void sendSupportMessage( String message, String name, String email, HttpServletRequest request,
                                    MultipartFile attachment ) throws MessagingException {
        StringBuilder content = new StringBuilder();
        content.append( "<p>Name: " + name + "</p>" );
        content.append( "<p>Email: " + email + "</p>" );
        content.append( "<p>User-Agent: " + request.getHeader( "User-Agent" ) + "</p>" );
        content.append( "<p>Message: " + message + "</p>" );
        boolean hasAttachment = (attachment != null && !Objects.equals( attachment.getOriginalFilename(), "" ));
        content.append( "<p>File Attached: " + hasAttachment + "</p>" );

        sendMessage( "IDR Bind Help - Contact Support", content.toString(), siteSettings.getContactEmail(), hasAttachment ? attachment : null );
    }

    public void sendJobSubmittedMessage( CCRSJob job ) throws MessagingException {
        if ( job.getEmail() == null || job.getEmail().isEmpty() ) {
            return;
        }
        sendMessage( "IDB Bind - Job Submitted", jobToEmail("Job Submitted", job), job.getEmail() );
    }

    public void sendJobStartMessage( CCRSJob job ) throws MessagingException {
        if ( job.getEmail() == null || job.getEmail().isEmpty() ) {
            return;
        }
        sendMessage( "IDB Bind - Job Started", jobToEmail("Job Started", job), job.getEmail() );

    }

    public void sendJobCompletionMessage( CCRSJob job ) throws MessagingException {
        if ( job.getEmail() == null || job.getEmail().isEmpty() ) {
            return;
        }
        sendMessage( "IDB Bind - Job Complete", jobToEmail("Job Complete", job), job.getEmail() );
    }

    private String jobToEmail(String header, CCRSJob job) {
        StringBuilder content = new StringBuilder();
        content.append( "<p>" + header + "</p>" );
        content.append( "<p>Label: " + job.getLabel() + "</p>" );
        content.append( "<p>Submitted: " + job.getSubmittedDate() + "</p>" );
        content.append( "<p>Status: " + job.getStatus() + "</p>" );
        if ( job.isSaved() ) {
            content.append( "<p>Saved Link: " + "<a href='" + siteSettings.getFullUrl()
                    + "job/" + job.getJobId() + "' target='_blank'>"
                    + siteSettings.getFullUrl() + "job/" + job.getJobId() + "'</a></p>" );
        }
        return content.toString();
    }

}