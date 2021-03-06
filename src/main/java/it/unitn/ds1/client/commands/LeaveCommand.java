package it.unitn.ds1.client.commands;

import akka.actor.ActorSelection;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import it.unitn.ds1.messages.client.ClientLeaveRequest;
import it.unitn.ds1.messages.client.ClientLeaveResponse;
import org.jetbrains.annotations.NotNull;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.concurrent.TimeUnit;

import static it.unitn.ds1.SystemConstants.CLIENT_TIMEOUT_SECONDS;

/**
 * Command to instruct the target actor to leave the system.
 */
public final class LeaveCommand implements Command {

	@NotNull
	@Override
	public CommandResult run(ActorSelection actor, String remote, LoggingAdapter logger) throws Exception {
		logger.info("[CLIENT] Asking node [{}] to leave...", remote);

		// instruct the target actor to leave the system
		final Timeout timeout = new Timeout(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		final Future<Object> future = Patterns.ask(actor, new ClientLeaveRequest(), timeout);

		// wait for an acknowledgement
		final Object message = Await.result(future, timeout.duration());
		assert message instanceof ClientLeaveResponse;

		// log the result
		logger.info("[CLIENT] Node [{} - {}] has successful left the system",
			((ClientLeaveResponse) message).getSenderID(), remote);

		// return a positive response... if there is a problem, an exception is thrown
		return new CommandResult(true, null);
	}
}
