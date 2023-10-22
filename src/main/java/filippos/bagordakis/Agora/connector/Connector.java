package filippos.bagordakis.Agora.connector;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;

import filippos.bagordakis.Agora.connector.client.AgoraClientInformation;
import filippos.bagordakis.agora.common.dto.AckoledgmentDTO;
import filippos.bagordakis.agora.common.dto.BaseDTO;
import filippos.bagordakis.agora.common.dto.GreetingDTO;
import filippos.bagordakis.agora.common.dto.HeartbeatDTO;
import filippos.bagordakis.agora.common.dto.RequestDTO;
import filippos.bagordakis.agora.common.request.cache.AgoraRequestCache;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class Connector {

	private static final Logger log = LoggerFactory.getLogger(Connector.class);

	private static final int PORT = 12345;

	private boolean running = false;

	private Map<AgoraClientInformation, ConcurrentLinkedQueue<BaseDTO>> que;
	private final List<Socket> sockets = new ArrayList<Socket>();

	public Connector() {

	}

	@PostConstruct
	public void start() throws JsonProcessingException {

		log.atInfo();

		que = new ConcurrentHashMap<AgoraClientInformation, ConcurrentLinkedQueue<BaseDTO>>();

		new Thread(() -> {
			log.info("Trying to open socket {}", PORT);
			try (ServerSocket serverSocket = new ServerSocket(PORT)) {
				running = true;
				log.info("Socket is open, Listening");
				while (running) {
					Socket clientSocket = serverSocket.accept();
					sockets.add(clientSocket);
					new Thread(new ClientHandler(clientSocket)).start();
				}
			} catch (Exception e) {
				log.error("Failed to open socket {}", PORT);
			}
		}).start();

	}

	@PreDestroy
	public void close() {
		running = false;
		sockets.forEach(t -> {
			try {
				t.close();
				log.info("Connection with [{}] was shut down", t.getRemoteSocketAddress());
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

	private class ClientHandler implements Runnable {

		private Socket socket;
		private Writer writer;
		private AgoraClientInformation agoraClientInformation;

		private final AgoraRequestCache cache = new AgoraRequestCache(Duration.ofMillis(2000), x -> {
			if (x instanceof RequestDTO dto) {
				log.info("Didnt hear back will reque !");
				que.get(agoraClientInformation).add(dto);
			}
		});

		public ClientHandler(Socket socket) {
			this.socket = socket;
		}

		@Override
		public void run() {
			log.info("Connected to client: " + socket.getRemoteSocketAddress());
			try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					ObjectInputStream reader = new ObjectInputStream(socket.getInputStream())) {

				BaseDTO dto;

				while ((dto = (BaseDTO) reader.readObject()) != null) {
					boolean sendAcknoledgment = true;
					Throwable error = null;
					if (dto instanceof GreetingDTO greetingDTO && agoraClientInformation == null) {

						log.info("Received a GreetingDTO [{}] with name [{}] from", greetingDTO.getId(),
								greetingDTO.getId());
						agoraClientInformation = new AgoraClientInformation(greetingDTO.getName(), UUID.randomUUID());
						que.put(agoraClientInformation, new ConcurrentLinkedQueue<BaseDTO>());

						writer = new Writer(out);
						new Thread(writer).start();
					} else if (dto instanceof HeartbeatDTO heartBeatDTO) {
						sendAcknoledgment = false;
						log.debug("Heartbeat [{}] received from [{}]", dto.getId(), socket.getRemoteSocketAddress());
						writer.sendHeartbeat(heartBeatDTO);
					} else if (dto instanceof RequestDTO requestDTO) {
						Stream<Entry<AgoraClientInformation, ConcurrentLinkedQueue<BaseDTO>>> stream = que.entrySet()
								.stream().filter(x -> !x.getKey().equals(agoraClientInformation));
						if (!requestDTO.getTarget().isEmpty()) {
							stream.filter(x -> requestDTO.getTarget().contains(x.getKey().agoraID()));
						}
						stream.forEach(x -> que.get(x.getKey()).add(requestDTO));
					} else if (dto instanceof AckoledgmentDTO acknoledgmentDTO) {
						sendAcknoledgment = false;
						cache.remove(acknoledgmentDTO);
					}

					if (sendAcknoledgment) {
						writer.sendAcknoledgment(dto.getId(), error);
					}
				}
			} catch (IOException | ClassNotFoundException e) {

			} finally {
				try {
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		private class Writer implements Runnable {

			private ObjectOutputStream out;

			public Writer(ObjectOutputStream out) {
				this.out = out;
			}

			@Override
			public void run() {

				log.info("Agora Writer started {}", socket.getRemoteSocketAddress());

				while (running) {

					if (!socket.isConnected() || socket.isClosed()) {
						running = false;
						log.error("Socket is not connected or closed. Stopping sending data.");
						break;
					}
					BaseDTO object = que.get(agoraClientInformation).poll();
					if (object != null) {
						try {
							cache.put(object);
							out.writeObject(object);
							log.info("Sent object [{}] over TCP", object.getClass());
						} catch (IOException e) {
							log.error("Failed to serialize and send object", e);
							running = false;
						}

					}

				}
			}

			public void sendHeartbeat(HeartbeatDTO heartbeat) {
				try {
					out.writeObject(heartbeat);
					log.debug("Heartbeat [{}] sent to [{}]", heartbeat.getId(), socket.getRemoteSocketAddress());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			public void sendAcknoledgment(UUID id, Throwable error) {
				AckoledgmentDTO ackoledgmentDTO = new AckoledgmentDTO(id, error);
				try {
					out.writeObject(ackoledgmentDTO);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

}
