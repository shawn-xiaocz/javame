package io.javame.adapter.http;

import io.javame.client.MatchingContracts.*;
import io.javame.service.MatchingService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

/** Authenticated service API; user IDs must come from a trusted upstream business service. */
@RestController
@RequestMapping("/v1")
public class MatchingController {
    private final MatchingService service;

    public MatchingController(MatchingService service) {
        this.service = service;
    }

    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public SubmitOrderResult place(@Valid @RequestBody PlaceOrderRequest request) {
        return service.place(request);
    }

    @PostMapping("/orders/cancel")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public SubmitOrderResult cancel(@Valid @RequestBody CancelOrderRequest request) {
        return service.cancel(request);
    }

    @GetMapping("/orders/{oid}")
    public GetOrderResult getOrder(@PathVariable String oid, @RequestParam String uuid, @RequestParam String symbol) {
        return service.getOrder(uuid, oid, symbol);
    }

    @GetMapping("/commands/{commandId}")
    public GetCommandResult getCommand(@PathVariable String commandId, @RequestParam String uuid) {
        return service.getCommand(uuid, commandId);
    }

    @GetMapping("/depth/{symbol}")
    public GetDepthResult getDepth(@PathVariable String symbol,
                                   @RequestParam(defaultValue = "DOUBLE_SIDE") String transaction,
                                   @RequestParam(defaultValue = "0") int offset,
                                   @RequestParam(defaultValue = "20") int count) {
        return service.getDepth(symbol, transaction, offset, count);
    }
}
