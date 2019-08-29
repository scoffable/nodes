package io.aexp.nodes.graphql;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.DefaultRequest;
import com.amazonaws.Response;
import com.amazonaws.SdkBaseException;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.http.*;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.aexp.nodes.graphql.exceptions.GraphQLException;
import io.aexp.nodes.graphql.internal.DefaultObjectMapperFactory;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class AppsyncIAMFetch implements Fetcher {

    private static Logger logger = Logger.getLogger(AppsyncIAMFetch.class);

    private final ObjectMapperFactory objectMapperFactory;
    private ObjectMapper mapper;
    private SimpleModule module;
    private static final int STATUS_CODE_THRESHOLD = 400;

    private String awsAccessKeyId;
    private String awsSecretKey;
    private String serviceName;
    private String awsRegion;

    public static final DateTimeFormatter AWS_DATE_FORMATTER = DateTimeFormatter.ofPattern("YYYYMMdd'T'hhmmss'Z'")
            .withZone(ZoneOffset.UTC);

    public AppsyncIAMFetch(){
        this.objectMapperFactory = new DefaultObjectMapperFactory();
    }

    public AppsyncIAMFetch setAwsAccessKeyId(String awsAccessKeyId) {
        this.awsAccessKeyId = awsAccessKeyId;
        return this;
    }

    public String getAwsAccessKeyId() {
        return awsAccessKeyId;
    }

    public AppsyncIAMFetch setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
        return this;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public AppsyncIAMFetch setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public AppsyncIAMFetch setAwsRegion(String awsRegion) {
        this.awsRegion = awsRegion;
        return this;
    }

    public <T> GraphQLResponseEntity<T> send(GraphQLRequestEntity requestEntity, Class<T> responseClass) throws GraphQLException {
        mapper = objectMapperFactory.newSerializerMapper();
        module = new SimpleModule();

        Request request = new Request();
        request.setQuery(requestEntity.getRequest());
        request.setVariables(requestEntity.getVariables());

        String responseMessage = null;
        String responseStatus = null;

        try {
            String requestParams = mapper.writeValueAsString(request);
            byte[] postData = requestParams.getBytes();

            // Send using AWS Libs
            Response<String> response = signAndSendRequestToAWS(requestEntity.getUrl(), requestParams, requestEntity.getHeaders());

            //Map<String, List<String>> responseHeaders = response.getAwsResponse()
            responseMessage = response.getHttpResponse().getStatusText();
            responseStatus = Integer.toString(response.getHttpResponse().getStatusCode());

            Wrapper<T> wrapper = deserializeResponse(response.getAwsResponse(), responseClass);

            if (response.getHttpResponse().getStatusCode() >= STATUS_CODE_THRESHOLD) {
                GraphQLException graphQLException = new GraphQLException(responseMessage);
                graphQLException.setStatus(responseStatus);
                graphQLException.setErrors(wrapper.getErrors());
                throw graphQLException;
            }

            GraphQLResponseEntity<T> graphQLResponseEntity = new GraphQLResponseEntity<T>();
            graphQLResponseEntity.setErrors(wrapper.getErrors());
            if (wrapper.getData() != null) {
                graphQLResponseEntity.setResponse(wrapper.getData().getResource());
            }
            return graphQLResponseEntity;
        } catch (Exception exception) {
            logger.error("Error sending request", exception);
            if (exception instanceof GraphQLException) throw (GraphQLException) exception;
            GraphQLException err = new GraphQLException();
            err.setStatus(responseStatus);
            err.setMessage(responseMessage);
            err.setDescription(exception.getMessage());
            throw err;
        }
    }

    private <T> Wrapper<T> deserializeResponse(String responseText, Class<T> responseClass) throws IOException {
        Deserializer<T> deserializer = new Deserializer<T>(responseClass, objectMapperFactory);
        module.addDeserializer(Resource.class, deserializer);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(module);
        return mapper.readValue(responseText, Wrapper.class);
    }

    /**
     * This signs the request and sends to AWS Appsync
     * @param requestUrl
     * @param content
     * @param headers
     * @return
     * @throws Exception
     */
    private Response<String> signAndSendRequestToAWS(URL requestUrl, String content, Map<String, String> headers) throws Exception{
        com.amazonaws.Request<Void> request = new DefaultRequest<>(getServiceName());
        request.setHttpMethod(HttpMethodName.POST);
        request.setEndpoint(requestUrl.toURI());

        request.addHeader("Content-Type", "application/json");
        request.addHeader("Accept", "application/json");
        request.addHeader("charset", "utf-8");

        for(String key : headers.keySet()){
            request.addHeader(key, headers.get(key));
        }

        request.setContent(new StringInputStream(content));

        // Sign the request
        AWS4Signer signer = new AWS4Signer();
        signer.setRegionName(getAwsRegion());
        signer.setServiceName(request.getServiceName());
        signer.sign(request, new BasicAWSCredentials(getAwsAccessKeyId(), getAwsSecretKey()));

        //Execute and get the response...
        Response<String> response = new AmazonHttpClient(new ClientConfiguration())
                .requestExecutionBuilder()
                .executionContext(new ExecutionContext(true))
                .request(request)
                .errorResponseHandler(new HttpResponseHandler<SdkBaseException>() {
                    @Override
                    public SdkBaseException handle(HttpResponse httpResponse) throws Exception {
                        logger.error("Error in response: "+ httpResponse.getStatusText());
                        try{
                            String body = IOUtils.toString(httpResponse.getContent(), StandardCharsets.UTF_8);
                            logger.error("Response text: "+body);
                        }
                        catch (Exception e){
                            logger.error("Could not convert response content to String", e);
                        }

                        throw new RuntimeException();
                    }

                    @Override
                    public boolean needsConnectionLeftOpen() {
                        return false;
                    }
                })
                .execute(new HttpResponseHandler<String>() {
                    @Override
                    public String handle(HttpResponse httpResponse) throws Exception {

                        try{
                            String body = IOUtils.toString(httpResponse.getContent(), StandardCharsets.UTF_8);
                            return body;
                        }
                        catch (Exception e){
                            logger.error("Could not convert response content to String", e);
                        }
                        return null;
                    }

                    @Override
                    public boolean needsConnectionLeftOpen() {
                        return false;
                    }
                });

        return response;
    }


}
