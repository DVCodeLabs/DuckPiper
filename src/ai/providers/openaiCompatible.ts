import { AIProvider } from './provider';

export class OpenAICompatibleProvider implements AIProvider {
    async complete(_prompt: string): Promise<string> { return ''; }
}
