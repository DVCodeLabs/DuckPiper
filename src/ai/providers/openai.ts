import { AIProvider } from './provider';

export class OpenAIProvider implements AIProvider {
    async complete(_prompt: string): Promise<string> { return ''; }
}
