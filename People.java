import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import com.google.gson.Gson;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.InvalidFormatException;
import opennlp.tools.util.Span;
	
public String getPeople(String sentence) {
		String outputJSON = "";
		TokenNameFinderModel model = null;
		try {
			model = new TokenNameFinderModel(
					new File("en-ner-person.bin"));
		} catch (InvalidFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		NameFinderME finder = new NameFinderME(model);
		Tokenizer tokenizer = SimpleTokenizer.INSTANCE;
		String[] tokens = tokenizer.tokenize(sentence);
		Span[] nameSpans = finder.find(tokens);
		List<PersonName> people = new ArrayList<PersonName>();
		String[] spanns = Span.spansToStrings(nameSpans, tokens);
		for (int i = 0; i < spanns.length; i++) {
			people.add(new PersonName(spanns[i]));
		}
 
		outputJSON = new Gson().toJson(people);
		finder.clearAdaptiveData();
		return "{\"names\":" + outputJSON + "}";
	}
