import { Authenticator, translations } from "@aws-amplify/ui-react";
import { I18n } from "aws-amplify/utils";
import "@aws-amplify/ui-react/styles.css";
import "./App.css";
import { Dashboard } from "./pages/Dashboard";

I18n.putVocabularies(translations);
I18n.setLanguage("es");

function App() {
  return (
    <Authenticator loginMechanisms={["email"]}>
      <Dashboard />
    </Authenticator>
  );
}

export default App;
