package filippos.bagordakis.Agora.connector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import filippos.bagordakis.agora.agora.AgoraHelper;
import filippos.bagordakis.agora.agora.data.dto.AckoledgmentDTO;
import filippos.bagordakis.agora.agora.data.dto.BaseDTO;
import filippos.bagordakis.agora.agora.data.dto.GreetingDTO;
import filippos.bagordakis.agora.agora.data.dto.HeartbeatDTO;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class Connector {

	private static final Logger log = LoggerFactory.getLogger(Connector.class);

	private static final int PORT = 12345;

	private final ObjectMapper objectMapper = AgoraHelper.getObjectMapper();

	private boolean running = false;

	private ConcurrentLinkedQueue<BaseDTO> que;

	@PostConstruct
	public void start() throws JsonProcessingException {

		que = new ConcurrentLinkedQueue<BaseDTO>();

		log.info("Trying to open socket {}", PORT);
		try (ServerSocket serverSocket = new ServerSocket(PORT)) {
			running = true;
			log.info("Socket is open, Listening");
			while (running) {
				Socket clientSocket = serverSocket.accept();
				new Thread(new ClientHandler(clientSocket)).start();
			}
		} catch (Exception e) {
			log.error("Failed to open socket {}", PORT);
		}

	}

	@PreDestroy
	public void close() {
		running = false;
	}

	private class ClientHandler implements Runnable {
		private Socket socket;
		private Writer writer;

		public ClientHandler(Socket socket) {
			this.socket = socket;
		}

		@Override
		public void run() {
			log.info("Connected to client: " + socket.getRemoteSocketAddress());
			try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
				String jsonLine;
				while ((jsonLine = reader.readLine()) != null) {
					boolean sendAcknoledgment = true;
					BaseDTO dto = objectMapper.readValue(jsonLine, BaseDTO.class);

					if (dto instanceof GreetingDTO greetingDTO) {
						log.info("Received a GreetingDTO [{}] with name [{}] from", greetingDTO.getId(),
								greetingDTO.getName());

						if (writer == null) {
							writer = new Writer(out);
							new Thread(new Writer(out)).start();
						}

					} else if (dto instanceof HeartbeatDTO heartBeatDTO) {
						sendAcknoledgment = false;
						log.info("Heartbeat [{}] received from [{}]", dto.getId(), socket.getRemoteSocketAddress());
						writer.sendHeartbeat(heartBeatDTO.getId(), jsonLine);
					}

					if (sendAcknoledgment) {
						writer.sendAcknoledgment(dto.getId());
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		private class Writer implements Runnable {

			private PrintWriter out;

			public Writer(PrintWriter out) {
				this.out = out;
			}

			@Override
			public void run() {

				log.info("Agora Writer started");

				while (running) {

					if (!socket.isConnected() || socket.isClosed()) {
						running = false;
						log.error("Socket is not connected or closed. Stopping sending data.");
						break;
					}
					Object object = que.poll();
					if (object != null) {
						try {
							String json = objectMapper.writeValueAsString(object);
							out.println(json);
							log.info("Sent object [{}] over TCP", json);
						} catch (IOException e) {
							log.error("Failed to serialize and send object", e);
							running = false;
						}

					}

					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

				}
			}

			public void sendHeartbeat(String id, String heartbeat) {
				out.println(heartbeat);
				log.info("Heartbeat [{}] sent to [{}]", id, socket.getRemoteSocketAddress());
			}

			public void sendAcknoledgment(String id) {
				AckoledgmentDTO ackoledgmentDTO = new AckoledgmentDTO(id);
				try {
					out.println(objectMapper.writeValueAsString(ackoledgmentDTO));
				} catch (JsonProcessingException e) {
					log.error("Failed to send acknoledgment [{}] to [{}]", id, socket.getRemoteSocketAddress());
					
				}

			}
		}

	}

}
