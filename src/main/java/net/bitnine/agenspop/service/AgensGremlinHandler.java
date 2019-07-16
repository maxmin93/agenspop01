package net.bitnine.agenspop.service;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.node.ArrayNode;
import org.apache.tinkerpop.shaded.jackson.databind.node.ObjectNode;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.codahale.metrics.MetricRegistry.name;

public class AgensGremlinHandler {

    private static final Logger logger = LoggerFactory.getLogger(AgensGremlinHandler.class);
    private static final Logger auditLogger = LoggerFactory.getLogger(GremlinServer.AUDIT_LOGGER_NAME);
    private static final Charset UTF8 = Charset.forName("UTF-8");
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));

    private static final String ARGS_BINDINGS_DOT = Tokens.ARGS_BINDINGS + ".";

    private static final String ARGS_ALIASES_DOT = Tokens.ARGS_ALIASES + ".";

    private static final Timer evalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "eval"));

    /**
     * Serializers for the response.
     */
    private final Map<String, MessageSerializer> serializers;

    /**
     * This is just a generic mapper to interpret the JSON of a POSTed request.  It is not used for the serialization
     * of the response.
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    private final GremlinExecutor gremlinExecutor;
    private final GraphManager graphManager;
    private final Settings settings;

    private static final Pattern pattern = Pattern.compile("(.*);q=(.*)");

    public AgensGremlinHandler(final Map<String, MessageSerializer> serializers,
                                      final GremlinExecutor gremlinExecutor,
                                      final GraphManager graphManager,
                                      final Settings settings) {
        this.serializers = serializers;
        this.gremlinExecutor = gremlinExecutor;
        this.graphManager = graphManager;
        this.settings = settings;
    }

    /////////////////////////////////////////

    public void runScript(final String msg) {
/*
        try {
            logger.debug("Processing request containing script [{}] and bindings of [{}] on {}",
                    requestArguments.getValue0(), requestArguments.getValue1(), Thread.currentThread().getName());
            if (settings.authentication.enableAuditLog) {
                String address = ctx.channel().remoteAddress().toString();
                if (address.startsWith("/") && address.length() > 1) address = address.substring(1);
                auditLogger.info("User with address {} requested: {}", address, requestArguments.getValue0());
            }
            final ChannelPromise promise = ctx.channel().newPromise();
            final AtomicReference<Object> resultHolder = new AtomicReference<>();
            promise.addListener(future -> {
                // if failed then the error was already written back to the client as part of the eval future
                // processing of the exception
                if (future.isSuccess()) {
                    logger.debug("Preparing HTTP response for request with script [{}] and bindings of [{}] with result of [{}] on [{}]",
                            requestArguments.getValue0(), requestArguments.getValue1(), resultHolder.get(), Thread.currentThread().getName());
                    final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, (ByteBuf) resultHolder.get());
                    response.headers().set(CONTENT_TYPE, serializer.getValue0());
                    response.headers().set(CONTENT_LENGTH, response.content().readableBytes());

                    // handle cors business
                    if (origin != null) response.headers().set(ACCESS_CONTROL_ALLOW_ORIGIN, origin);

                    if (!keepAlive) {
                        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                    } else {
                        response.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                        ctx.writeAndFlush(response);
                    }
                }
            });

            final Timer.Context timerContext = evalOpTimer.time();

            final Bindings bindings;
            try {
                bindings = createBindings(requestArguments.getValue1(), requestArguments.getValue3());
            } catch (IllegalStateException iae) {
                sendError(ctx, BAD_REQUEST, iae.getMessage());
                ReferenceCountUtil.release(msg);
                return;
            }

            // provide a transform function to serialize to message - this will force serialization to occur
            // in the same thread as the eval. after the CompletableFuture is returned from the eval the result
            // is ready to be written as a ByteBuf directly to the response.  nothing should be blocking here.
            final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(requestArguments.getValue0(), requestArguments.getValue2(), bindings,
                    FunctionUtils.wrapFunction(o -> {
                        // stopping the timer here is roughly equivalent to where the timer would have been stopped for
                        // this metric in other contexts.  we just want to measure eval time not serialization time.
                        timerContext.stop();

                        logger.debug("Transforming result of request with script [{}] and bindings of [{}] with result of [{}] on [{}]",
                                requestArguments.getValue0(), requestArguments.getValue1(), o, Thread.currentThread().getName());
                        final ResponseMessage responseMessage = ResponseMessage.build(UUID.randomUUID())
                                .code(ResponseStatusCode.SUCCESS)
                                .result(IteratorUtils.asList(o)).create();

                        // http server is sessionless and must handle commit on transactions. the commit occurs
                        // before serialization to be consistent with how things work for websocket based
                        // communication.  this means that failed serialization does not mean that you won't get
                        // a commit to the database
                        attemptCommit(requestArguments.getValue3(), graphManager, settings.strictTransactionManagement);

                        try {
                            return Unpooled.wrappedBuffer(serializer.getValue1().serializeResponseAsString(responseMessage).getBytes(UTF8));
                        } catch (Exception ex) {
                            logger.warn(String.format("Error during serialization for %s", responseMessage), ex);
                            throw ex;
                        }
                    }));

            evalFuture.exceptionally(t -> {
                if (t.getMessage() != null)
                    sendError(ctx, INTERNAL_SERVER_ERROR, t.getMessage(), Optional.of(t));
                else
                    sendError(ctx, INTERNAL_SERVER_ERROR, String.format("Error encountered evaluating script: %s", requestArguments.getValue0())
                            , Optional.of(t));
                promise.setFailure(t);
                return null;
            });

            evalFuture.thenAcceptAsync(r -> {
                // now that the eval/serialization is done in the same thread - complete the promise so we can
                // write back the HTTP response on the same thread as the original request
                resultHolder.set(r);
                promise.setSuccess();
            }, gremlinExecutor.getExecutorService());
        } catch (Exception ex) {
            // tossed to exceptionCaught which delegates to sendError method
            final Throwable t = ExceptionUtils.getRootCause(ex);
            throw new RuntimeException(null == t ? ex : t);
        }
*/
    }

    private Bindings createBindings(final Map<String,Object> bindingMap, final Map<String,String> rebindingMap)  {
        final Bindings bindings = new SimpleBindings();

        // rebind any global bindings to a different variable.
        if (!rebindingMap.isEmpty()) {
            for (Map.Entry<String, String> kv : rebindingMap.entrySet()) {
                boolean found = false;
                final Graph g = this.graphManager.getGraph(kv.getValue());
                if (null != g) {
                    bindings.put(kv.getKey(), g);
                    found = true;
                }

                if (!found) {
                    final TraversalSource ts = this.graphManager.getTraversalSource(kv.getValue());
                    if (null != ts) {
                        bindings.put(kv.getKey(), ts);
                        found = true;
                    }
                }

                if (!found) {
                    final String error = String.format("Could not rebind [%s] to [%s] as [%s] not in the Graph or TraversalSource global bindings",
                            kv.getKey(), kv.getValue(), kv.getValue());
                    throw new IllegalStateException(error);
                }
            }
        }

        bindings.putAll(bindingMap);

        return bindings;
    }

    private Pair<String,MessageTextSerializer> chooseSerializer(final String acceptString) {
        final List<Pair<String,Double>> ordered = Stream.of(acceptString.split(",")).map(mediaType -> {
            // parse out each mediaType with its params - keeping it simple and just looking for "quality".  if
            // that value isn't there, default it to 1.0.  not really validating here so users better get their
            // accept headers straight
            final Matcher matcher = pattern.matcher(mediaType);
            return (matcher.matches()) ? Pair.with(matcher.group(1), Double.parseDouble(matcher.group(2))) : Pair.with(mediaType, 1.0);
        }).sorted((o1, o2) -> o2.getValue0().compareTo(o1.getValue0())).collect(Collectors.toList());

        for (Pair<String,Double> p : ordered) {
            // this isn't perfect as it doesn't really account for wildcards.  that level of complexity doesn't seem
            // super useful for gremlin server really.
            final String accept = p.getValue0().equals("*/*") ? "application/json" : p.getValue0();
            if (serializers.containsKey(accept))
                return Pair.with(accept, (MessageTextSerializer) serializers.get(accept));
        }

        return null;
    }

    public static Object fromJsonNode(final JsonNode node) {
        if (node.isNull())
            return null;
        else if (node.isObject()) {
            final Map<String, Object> map = new HashMap<>();
            final ObjectNode objectNode = (ObjectNode) node;
            final Iterator<String> iterator = objectNode.fieldNames();
            while (iterator.hasNext()) {
                String key = iterator.next();
                map.put(key, fromJsonNode(objectNode.get(key)));
            }
            return map;
        } else if (node.isArray()) {
            final ArrayNode arrayNode = (ArrayNode) node;
            final ArrayList<Object> array = new ArrayList<>();
            for (int i = 0; i < arrayNode.size(); i++) {
                array.add(fromJsonNode(arrayNode.get(i)));
            }
            return array;
        } else if (node.isFloatingPointNumber())
            return node.asDouble();
        else if (node.isIntegralNumber())
            return node.asLong();
        else if (node.isBoolean())
            return node.asBoolean();
        else
            return node.asText();
    }

    private static void attemptCommit(final Map<String, String> aliases, final GraphManager graphManager, final boolean strict) {
        if (strict)
            graphManager.commit(new HashSet<>(aliases.values()));
        else
            graphManager.commitAll();
    }

}
