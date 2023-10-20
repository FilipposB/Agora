package filippos.bagordakis.Agora.connector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;

import filippos.bagordakis.agora.agora.data.dto.BaseDTO;
import filippos.bagordakis.agora.agora.data.dto.GreetingDTO;
import filippos.bagordakis.agora.agora.data.dto.HeartbeatDTO;
import jakarta.annotation.PostConstruct;

@Service
public class Connector {

	private static final Logger log = LoggerFactory.getLogger(Connector.class);

	private static final int PORT = 12345;

	private ObjectMapper objectMapper;

	private boolean running = false;

	@PostConstruct
	public void start() {

		this.objectMapper = new ObjectMapper();
		objectMapper.registerSubtypes(new NamedType(GreetingDTO.class, "greeting"),  new NamedType(HeartbeatDTO.class, "heartbeat"));

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

	private class ClientHandler implements Runnable {
		private Socket socket;

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
					BaseDTO dto = objectMapper.readValue(jsonLine, BaseDTO.class);

					if (dto instanceof GreetingDTO) {
						GreetingDTO greetingDTO = (GreetingDTO) dto;
						log.info("Received a GreetingDTO with name [{}] from", greetingDTO.getName());
					} else if (dto instanceof HeartbeatDTO) {
						log.info("Heartbeat");
						out.println(jsonLine);
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
	}
}
