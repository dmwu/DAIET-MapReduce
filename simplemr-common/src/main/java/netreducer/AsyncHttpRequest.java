package netreducer;

import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncHttpRequest implements Closeable{
	
	private ExecutorService executor;
	private Future<HttpResponse> response;
	private HttpGet request;
	private CloseableHttpClient httpClient;
	private static Logger LOG = LoggerFactory.getLogger(AsyncHttpRequest.class);
	
	public AsyncHttpRequest(){
		executor = Executors.newFixedThreadPool(1);
		request = null;
		response = null;
		httpClient = HttpClients.createDefault();
	}
	
	
	class HttpTask implements Callable<HttpResponse>{
		
		private CloseableHttpClient httpClient;
		private HttpGet get;
		
		public HttpTask(String url, CloseableHttpClient httpClient){
			this.get = new HttpGet(url);
			this.httpClient = httpClient;
		}
		
		public HttpTask(URI uri, CloseableHttpClient httpClient){
			this.get = new HttpGet(uri);
			this.httpClient = httpClient;
		}
		
		public HttpGet getGet() {
			return get;
		}
		
		@Override
		public HttpResponse call() throws Exception {
			
			return httpClient.execute(get);
		}
	}
	
	public void asyncHttpGet (String url){
		
		HttpTask httpTask = new HttpTask(url, httpClient);
		
		request = httpTask.getGet();
		response = executor.submit(httpTask);
	}

	public void asyncHttpGet (String url, Header[] headers){
		
		HttpTask httpTask = new HttpTask(url, httpClient);
		
		request = httpTask.getGet();
		
		if (headers!=null)
			request.setHeaders(headers);
		
		response = executor.submit(httpTask);
	}

	public void asyncHttpGet (URI uri){
		
		HttpTask httpTask = new HttpTask(uri, httpClient);
		
		request = httpTask.getGet();
		response = executor.submit(httpTask);
	}

	public void asyncHttpGet (URI uri, Header[] headers){
		
		HttpTask httpTask = new HttpTask(uri, httpClient);
		
		request = httpTask.getGet();
		
		if (headers!=null)
			request.setHeaders(headers);
		
		response = executor.submit(httpTask);
	}
	
	public HttpResponse getResponse() throws InterruptedException, ExecutionException{
		return response.get();
	}
	
	public HttpGet getRequest(){
		return request;
	}
	
	public void close(){
		executor.shutdown();
	}
	
	protected void finalize() throws Throwable {
		try {
			httpClient.close();
			close();
		} finally {
			super.finalize();
		}
	}
}
