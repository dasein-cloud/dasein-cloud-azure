package org.dasein.cloud.azure.platform;

import org.apache.http.client.methods.RequestBuilder;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.azure.Azure;
import org.dasein.cloud.azure.AzureConfigException;
import org.dasein.cloud.azure.platform.model.CreateDatabaseRestoreModel;
import org.dasein.cloud.azure.platform.model.DatabaseServiceResourceModel;
import org.dasein.cloud.azure.platform.model.ServerModel;
import org.dasein.cloud.azure.platform.model.ServerServiceResourceModel;
import org.dasein.cloud.util.requester.entities.DaseinObjectToXmlEntity;

import java.net.*;

/**
 * Created by Vlad_Munthiu on 11/19/2014.
 */
public class AzureSQLDatabaseSupportRequests{
    private Azure provider;

    private final String RESOURCE_SERVERS = "/services/sqlservers/servers?contentview=generic";
    private final String RESOURCE_SERVERS_NONGEN = "/services/sqlservers/servers";
    private final String RESOURCE_SERVER = "/services/sqlservers/servers/%s";
    private final String RESOURCE_DATABASES = "/services/sqlservers/servers/%s/databases";
    private final String RESOURCE_DATABASE = "/services/sqlservers/servers/%s/databases/%s";
    private final String RESOURCE_LIST_DATABASES = "/services/sqlservers/servers/%s/databases?contentview=generic";
    private final String RESOURCE_SUBSCRIPTION_META = "/services/sqlservers/subscriptioninfo";
    private final String RESOURCE_LIST_RECOVERABLE_DATABASES = "/services/sqlservers/servers/%s/recoverabledatabases?contentview=generic";
    private final String RESOURCE_RESTORE_DATABASE_OPERATIONS = "/services/sqlservers/servers/%s/restoredatabaseoperations";
    private final String RESOURCE_SERVER_FIREWALL = "/services/sqlservers/servers/%s/firewallrules";
    private final String RESOURCE_FIREWALL_RULE = "/services/sqlservers/servers/%s/firewallrules/%s";

    public AzureSQLDatabaseSupportRequests(Azure provider){
        this.provider = provider;
    }

    public RequestBuilder createServer(ServerModel serverToCreate) throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.post();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder.setUri(getEndpoint() + RESOURCE_SERVERS);
        requestBuilder.setEntity(new DaseinObjectToXmlEntity<ServerModel>(serverToCreate));
        return requestBuilder;
    }

    public RequestBuilder listServers() throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.get();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder.setUri(getEndpoint() + RESOURCE_SERVERS);
        return requestBuilder;
    }

    public RequestBuilder listServersNonGen() throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.get();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder.setUri(getEndpoint() + RESOURCE_SERVERS_NONGEN);
        return requestBuilder;
    }

    public RequestBuilder deleteServer(String serverName) throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.delete();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder.setUri(String.format(getEndpoint() + RESOURCE_SERVER, serverName));
        return requestBuilder;
    }

    public RequestBuilder createDatabase(String serverName, DatabaseServiceResourceModel dbToCreate)
            throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.post();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder.setUri(String.format(getEndpoint() + RESOURCE_DATABASES, serverName));
        requestBuilder.setEntity(new DaseinObjectToXmlEntity<DatabaseServiceResourceModel>(dbToCreate));
        return requestBuilder;
    }

    public RequestBuilder createDatabaseFromBackup(String serverName,
            CreateDatabaseRestoreModel createDatabaseRestoreModel) throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.post();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder.setUri(String.format(getEndpoint() + RESOURCE_RESTORE_DATABASE_OPERATIONS, serverName));
        requestBuilder.setEntity(new DaseinObjectToXmlEntity<CreateDatabaseRestoreModel>(createDatabaseRestoreModel));
        return requestBuilder;
    }

    public RequestBuilder listDatabases(String serverName) throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.get();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder.setUri(String.format(getEndpoint() + RESOURCE_LIST_DATABASES, serverName));
        return requestBuilder;
    }

    public RequestBuilder deleteDatabase(String serverName, String databaseName) throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.delete();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder
                .setUri(getEncodedUri(String.format(getEndpoint() + RESOURCE_DATABASE, serverName, databaseName)));
        return requestBuilder;
    }
    public RequestBuilder subscriptionMetaRequest() throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.get();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder.setUri(getEndpoint() + RESOURCE_SUBSCRIPTION_META);
        return requestBuilder;
    }

    public RequestBuilder getDatabase(String serverName, String databaseName) throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.get();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder
                .setUri(getEncodedUri(String.format(getEndpoint() + RESOURCE_DATABASE, serverName, databaseName)));
        return requestBuilder;
    }

    public RequestBuilder getRecoverableDatabases(String serverName) throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.get();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder.setUri(String.format(getEndpoint() + RESOURCE_LIST_RECOVERABLE_DATABASES, serverName));
        return requestBuilder;
    }

    public RequestBuilder addFirewallRule(String serverName, ServerServiceResourceModel firewallRule)
            throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.post();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder.setUri(String.format(getEndpoint() + RESOURCE_SERVER_FIREWALL, serverName));
        requestBuilder.setEntity(new DaseinObjectToXmlEntity<ServerServiceResourceModel>(firewallRule));
        return requestBuilder;
    }

    public RequestBuilder listFirewallRules(String serveName) throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.get();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder.setUri(String.format(getEndpoint() + RESOURCE_SERVER_FIREWALL, serveName));
        return requestBuilder;
    }

    public RequestBuilder deleteFirewallRule(String serverName, String ruleName) throws InternalException {
        RequestBuilder requestBuilder = RequestBuilder.delete();
        addAzureCommonHeaders(requestBuilder);
        requestBuilder
                .setUri(getEncodedUri(String.format(getEndpoint() + RESOURCE_FIREWALL_RULE, serverName, ruleName)));
        return requestBuilder;
    }

    private String getEndpoint() throws InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new InternalException("No context was provided for this request");
        }
        String endpoint = ctx.getCloud().getEndpoint();
        if( endpoint == null ) {
            throw new InternalException("No endpoint was provided for this request");
        }

        if(endpoint.endsWith("/") ) {
            endpoint = endpoint + ctx.getAccountNumber();
        } else {
            endpoint = endpoint + "/" + ctx.getAccountNumber();
        }

        return endpoint;
    }

    private String getEncodedUri(String urlString) throws InternalException {
        try {
            URL url = new URL(urlString);
            return new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef()).toString();
        } catch (Exception e) {
            throw new InternalException(e.getMessage());
        }
    }

    private void addAzureCommonHeaders(RequestBuilder requestBuilder){
        requestBuilder.addHeader("x-ms-version", "2012-03-01");
        requestBuilder.addHeader("Content-Type", "application/xml;charset=UTF-8");
    }
}
